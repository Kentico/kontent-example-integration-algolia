/* The principle of modeling modular content for search lies in the SLUG property
 * The assumption being that every item that has a SLUG property
 * is being rendered on the website as a separate page, thus pages with slug are marked
 * as the ones that are being searched for + linked items with SLUG are being ignored
 * while the content of a page is being modeled */

/* If your model does not support this assumption, you have to remodel the processing
 * part of the function based on your model (i.e. use taxonomies instead of the slug property)
 * in order to model your data correctly inside of the search engine */
import { DeliveryClient, IContentItem } from "@kontent-ai/delivery-sdk";
import { Handler } from "@netlify/functions";
import createAlgoliaClient from "algoliasearch";

import { customUserAgent } from "../shared/algoliaUserAgent";
import { findMissingInitRequestBodyProps, isValidInitRequestBody } from "../shared/types/initRequestBody";
import { canConvertToAlgoliaItem, convertToAlgoliaItem } from "./utils/algoliaItem";
import { createEnvVars } from "./utils/createEnvVars";
import { sdkHeaders } from "./utils/sdkHeaders";
import { serializeUncaughtErrorsHandler } from "./utils/serializeUncaughtErrorsHandler";

const { envVars, missingEnvVars } = createEnvVars(["ALGOLIA_API_KEY"] as const);

export const handler: Handler = serializeUncaughtErrorsHandler(async (event) => {
  console.log("env vars", envVars);
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }

  const body = JSON.parse(event.body || "null");
  if (!isValidInitRequestBody(body)) {
    return {
      statusCode: 400,
      body: `Missing or invalid body, the following properties are missing or invalid: ${
        findMissingInitRequestBodyProps(body).join(", ")
      }`,
    };
  }
  if (!envVars.ALGOLIA_API_KEY) {
    return {
      statusCode: 500,
      body: `${missingEnvVars.join(", ")} environment variable(s) are missing, please check the documentation`,
    };
  }

  const deliverClient = new DeliveryClient({ environmentId: body.projectId, globalHeaders: () => sdkHeaders });
  const allItems = await getAllContentFromProject(deliverClient, body.language);
  const allItemsMap = new Map(allItems.map(i => [i.system.codename, i]));
  const recordItems = allItems
    .filter(canConvertToAlgoliaItem(body.slugCodename))
    .map(convertToAlgoliaItem(allItemsMap, body.slugCodename));

  const algoliaClient = createAlgoliaClient(body.algoliaAppId, envVars.ALGOLIA_API_KEY, { userAgent: customUserAgent });
  const index = algoliaClient.initIndex(body.algoliaIndexName);
  await index.setSettings({
    searchableAttributes: ["content.contents", "content.name", "name"],
    attributesForFaceting: ["content.codename", "language"],
    attributesToSnippet: ["content.contents:80"],
  }).wait();
  const result = await index.saveObjects(recordItems).wait();

  return {
    statusCode: 200,
    body: JSON.stringify(result.objectIDs),
  };
});

const getAllContentFromProject = async (
  deliverClient: DeliveryClient,
  languageCodename: string,
): Promise<IContentItem[]> => {
  const feed = await deliverClient.items().queryConfig({ waitForLoadingNewContent: true })
    .languageParameter(languageCodename).equalsFilter("system.language", languageCodename).toPromise();

  return [...feed.data.items, ...Object.values(feed.data.linkedItems)];
};
