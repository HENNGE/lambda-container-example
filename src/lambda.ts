import {AttributeValue, DynamoDBStreamHandler} from "aws-lambda";
import algolia from "algoliasearch";

export const lambda: DynamoDBStreamHandler = async (event, context, callback) => {
  const work: Work = {};

  for (const r of event.Records) {
    if (r.dynamodb?.StreamViewType !== "NEW_AND_OLD_IMAGES" ||
        !r.eventSourceARN ||
        !r.dynamodb.Keys ||
        !r.dynamodb.Keys.H.S) {
      console.error("event stream is not configured correctly", r);
      continue;
    }

    const rere = /^.*:table[/](?<table_name>[^/]+)[/]stream[/].*$/;
    const { table_name } = r.eventSourceARN.match(rere)?.groups || {};

    // you may want to dispatch on table name,
    // then you could use same function with multiple, heterogenous tables
    if (table_name?.endsWith("FIXME-ignore")) {
      console.debug("data from unexpected table", table_name);
      continue;
    }

    // you may want to adjust the hash and range key field names here
    const { H, R } = r.dynamodb.Keys;
    if (!H || !H.S || !R || !R.S) {
      console.error("broken record", r);
      continue;
    }

    const hash = H.S;
    const range = R.S;
    // you may want to ignore some classes of items here
    if (hash.endsWith("FIXME-ignore")) {
      console.debug("unexpected item", { hash, range });
      continue;
    }

    if (!hash || !range) {
      console.error("should not happen", r.dynamodb.Keys);
      continue;
    }

    // some canonical, unique id to map Algolia search hits back to table rows
    const id = `${ hash }|${ range }`;

    // it's highly recommended to map raw dynamo data to some data model
    const old = Model(hash, range, r.dynamodb.OldImage);
    const neu = Model(hash, range, r.dynamodb.NewImage);

    // this never happens
    switch (r.eventName) {
      case "INSERT":
        if (!!old || !neu) {
          console.error("weird insert data", r);
          continue;
        }
        break;
      case "MODIFY":
        if (!old || !neu) {
          console.error("weird modify data", r);
          continue;
        }
        break;
      case "REMOVE":
        if (!old || !!neu) {
          console.error("weird delete data", r)
          continue;
        }
        break;
    }

    // compress [potentially] multiple action on same row into one
    work[id] = { old: id in work? work[id].old: old, neu };
  }

  const nothing = filterWork(work, ([, w]) => !w.old && !w.neu);
  const additions = filterWork(work, ([, w]) => !w.old && !!w.neu);
  const deletions = filterWork(work, ([, w]) => !!w.old && !w.neu);
  const edits = filterWork(work, ([, w]) => !!w.old && !!w.neu);
  const updates = filterWork(edits, ([, w]) => !isEquivalent(w!.old, w?.neu));

  if (!process.env.ALGOLIA_APP_ID || !process.env.ALGOLIA_API_KEY) {
    console.error("need environment variables", "ALGOLIA_APP_ID", "ALGOLIA_API_KEY");
    return;
  }

  const client = algolia(process.env.ALGOLIA_APP_ID, process.env.ALGOLIA_API_KEY, {});
  const idx = client.initIndex("FIXME-my-search-index-name");  // change the name
  const stats = {
    records: event.Records.length,
    work: Object.keys(work).length,
    nothing: Object.keys(nothing).length,
    edits: Object.keys(edits).length,
    deletions: Object.keys(deletions).length,
    additions: Object.keys(additions).length,
    updates: Object.keys(updates).length,
  }
  console.log("STATS", JSON.stringify(stats));  // track if the fancy algorithm is even needed
  const tmp = await idx.batch([
    ...Object.keys(deletions).map(objectID =>
      ({ action: "deleteObject" as "deleteObject", body: { objectID } })),
    ...Object.entries(additions).map(([objectID, data]) =>
      ({ action: "addObject" as "addObject", body: { objectID, ...data.neu! } })),
    ...Object.entries(updates).map(([objectID, data]) =>
      ({ action: "partialUpdateObject" as "partialUpdateObject", body: { objectID, ...delta(data.old!, data.neu!) } })),
  ]);
  console.log("RESULT", { ids: tmp.objectIDs.length });
};

// you most certainly want to adjust this
const Model = (hash: string, range: string, row: { [key: string]: AttributeValue | undefined } | undefined) => {
  if (!row) return null;
  return {
    hash,  // you may want to give semantic
    range,  // names to these fields
    a_text: row.a_text?.S?.normalize() || "",
    an_array: row.an_array?.NS?.map(s => s.normalize()) || [],
    a_boolean: boolify(row.a_boolean),
    an_integer: intify(row.an_integer, null),
    a_convered_array: make_array(row.a_mapping),
  }
}

type ExcludesFalsey = <T>(x: T | false | undefined) => x is T;     

const make_array = (v: AttributeValue | undefined) => {
  if (!v) return [];
  if (!v.M) return [];
  return Object.values(v.M).map(e => e.S).filter(Boolean as any as ExcludesFalsey).map(s => s.normalize());
};

const intify = <T>(v: AttributeValue | undefined, fallback: T) => {
  if (!v) return fallback;
  if ("N" in v) return parseInt(v.N!);  // if some numeric values are saved as strings
  if ("S" in v && v.S?.match("^\d+$")) return parseInt(v.S);
  return fallback;
};

const boolify = (v: AttributeValue | undefined) => {
  if (!v) return false;
  if ("BOOL" in v) return v.BOOL!;
  if ("S" in v) return v.S === "1";  // whatever convention you have for other types
  return false;
}

type WorkItem = {
  old?: ReturnType<typeof Model>;
  neu?: ReturnType<typeof Model>;
}

type Work = { [key: string]: WorkItem };

const filterWork = (obj: Work, filter: (arg: [string, WorkItem]) => boolean) =>
  Object.fromEntries(Object.entries(obj).filter(filter));

const zip = <T>(a: Array<T>, b: Array<T>) => a.map((e, i) => [e, b[i]]);

type JSON = null | undefined | boolean | string | number | Array<JSON> | { [key: string]: JSON };

// you may want to customise the "same field value" function
// in which case, consider writing unit tests for it ;-)
const isEquivalent = (a: JSON, b: JSON): boolean => {
  if (typeof a !== typeof b) return false;
  if (["number", "boolean", "undefined", "symbol", "string"].includes(typeof a))
    return a === b;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a) && Array.isArray(b) && a.length === b.length) {
    for (const [aa, bb] of zip(a.concat().sort(), b.concat().sort()))
      if (!isEquivalent(aa, bb)) return false;
    return true;
  }
  if (!a || !b) return a === b;
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  return (
    ka.length === kb.length &&
    zip(ka, kb).every(([aa, bb]) => aa === bb) &&
    zip(Object.values(a), Object.values(b)).every(([aa, bb]) =>
      isEquivalent(aa, bb),
    )
  );
};

type ModelType = NonNullable<ReturnType<typeof Model>>;

// what fields changed from a to b
const delta = (a: ModelType, b: ModelType): Partial<typeof Model> =>
  Object.fromEntries((Object.keys(b) as (keyof ModelType)[]).filter(k => !isEquivalent(a[k], b[k])).map(k => [k, b[k]]));