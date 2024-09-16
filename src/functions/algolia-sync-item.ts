import { Contracts, DeliveryClient, IContentItem } from "@kontent-ai/delivery-sdk";
import { Handler } from "@netlify/functions";
import createAlgoliaClient from "algoliasearch";
import { z } from "zod";

import { customUserAgent } from "../shared/algoliaUserAgent";
import { canConvertToAlgoliaItem, convertToAlgoliaItem } from "./utils/algoliaItem";
import { createEnvVars } from "./utils/createEnvVars";
import { serializeUncaughtErrorsHandler } from "./utils/serializeUncaughtErrorsHandler";

const { envVars } = createEnvVars(["KONTENT_SECRET", "ALGOLIA_API_KEY"] as const);

export const handler: Handler = serializeUncaughtErrorsHandler(async (event) => {
  const body = bodySchema.parse(JSON.parse(event.body ?? "{}"));

  const affectedItems = await searchInParents({
    fetchParents: async itemCodename => {
      const result = await getItemParents(itemCodename, body.environmentId);

      if (!Array.isArray(result)) {
        throw new Error(`Failed to fetch parents for item with codename ${itemCodename}`);
      }

      return result;
    },
    isTargetItem: item => canConvertToAlgoliaItem(body.slug)(item as any),
  })([body.itemCodename]);

  const client = new DeliveryClient({ environmentId: body.environmentId });

  const itemsToUpdate = await Promise.all(
    affectedItems.map(async item => {
      const children = await findDeliverItemWithChildrenByCodename(client, item.system.codename, item.system.language);
      return convertToAlgoliaItem(children, body.slug)(children.get(item.system.codename) as IContentItem);
    }),
  );

  const algoliaClient = createAlgoliaClient(body.algoliaAppId, envVars.ALGOLIA_API_KEY ?? "", {
    userAgent: customUserAgent,
  });
  const index = algoliaClient.initIndex(body.indexName);

  await index.saveObjects(itemsToUpdate).wait();

  return {
    statusCode: 200,
    body: JSON.stringify(itemsToUpdate),
  };
});

const getItemParents = async (itemCodename: string, environmentId: string) => {
  const result = await fetch(
    `https://deliver.kontent.ai/${environmentId}/early-access/used-in/complete-item/items/${itemCodename}`,
  )
    .then(res => res.json());

  const parseResult = responseSchema.safeParse(result);

  if (!parseResult.success) {
    return parseResult.error;
  }

  return parseResult.data.items as ReadonlyArray<Contracts.IContentItemContract>;
};

const bodySchema = z.object({
  environmentId: z.string(),
  itemCodename: z.string(),
  slug: z.string(),
  indexName: z.string(),
  algoliaAppId: z.string(),
});

const responseSchema = z.object({
  items: z.array(z.object({
    system: z.object({
      id: z.string(),
      name: z.string(),
      codename: z.string(),
      language: z.string(),
      type: z.string(),
      collection: z.string(),
      sitemap_locations: z.array(z.string()),
      last_modified: z.string(),
      workflow_step: z.string(),
      workflow: z.string(),
    }),
    elements: z.record(z.any()),
  })),
  pagination: z.object({
    skip: z.number(),
    limit: z.number(),
    count: z.number(),
    next_page: z.string(),
  }),
});

type ParentsSearchParams = Readonly<{
  fetchParents: (itemCodename: string) => Promise<ReadonlyArray<Contracts.IContentItemContract>>;
  isTargetItem: (item: Contracts.IContentItemContract) => boolean;
}>;

const searchInParents = (params: ParentsSearchParams) =>
async (
  searchFromItemCodenames: ReadonlyArray<string>,
  visitedCodenames: ReadonlySet<string> = new Set(),
): Promise<ReadonlyArray<Contracts.IContentItemContract>> => {
  const nonUniqueParents = (await Promise.all(searchFromItemCodenames.map(params.fetchParents))).flat();

  const parents = uniqueItems(nonUniqueParents);

  const foundTargets = parents.filter(params.isTargetItem);

  const newSearchFromItemCodenames = parents
    .filter(item => !visitedCodenames.has(item.system.codename))
    .filter(item => !foundTargets.some(target => target.system.codename === item.system.codename))
    .map(item => item.system.codename);

  const newVisitedCodenames = new Set([
    ...visitedCodenames,
    ...searchFromItemCodenames,
    ...foundTargets.map(i => i.system.codename),
  ]);

  if (!newSearchFromItemCodenames.length) {
    return foundTargets;
  }

  const restTargets = await searchInParents(params)(newSearchFromItemCodenames, newVisitedCodenames);

  return uniqueItems([...foundTargets, ...restTargets]);
};

const uniqueItems = (items: ReadonlyArray<Contracts.IContentItemContract>) =>
  Array.from(new Map(items.map(i => [i.system.codename, i] as const)).values());

const findDeliverItemWithChildrenByCodename = async (
  deliverClient: DeliveryClient,
  codename: string,
  languageCodename: string,
): Promise<ReadonlyMap<string, IContentItem>> => {
  try {
    const response = await deliverClient
      .item(codename)
      .queryConfig({ waitForLoadingNewContent: true })
      .languageParameter(languageCodename)
      .depthParameter(100)
      .toPromise();

    return new Map([response.data.item, ...Object.values(response.data.linkedItems)].map(i => [i.system.codename, i]));
  } catch {
    return new Map();
  }
};
