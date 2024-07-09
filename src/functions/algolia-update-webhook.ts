import { DeliveryClient, IContentItem } from "@kontent-ai/delivery-sdk";
import {
  SignatureHelper,
  WebhookItemNotification,
  WebhookNotification,
  WebhookResponse,
} from "@kontent-ai/webhook-helper";
import { Handler } from "@netlify/functions";
import createAlgoliaClient, { SearchIndex } from "algoliasearch";

import { customUserAgent } from "../shared/algoliaUserAgent";
import { hasStringProperty, nameOf } from "../shared/utils/typeguards";
import { AlgoliaItem, canConvertToAlgoliaItem, convertToAlgoliaItem } from "./utils/algoliaItem";
import { createEnvVars } from "./utils/createEnvVars";
import { sdkHeaders } from "./utils/sdkHeaders";
import { serializeUncaughtErrorsHandler } from "./utils/serializeUncaughtErrorsHandler";

const { envVars, missingEnvVars } = createEnvVars(["KONTENT_SECRET", "ALGOLIA_API_KEY"] as const);

const signatureHeaderName = "x-kontent-ai-signature";

export const handler: Handler = serializeUncaughtErrorsHandler(async (event) => {
  if (event.httpMethod !== "POST") {
    return { statusCode: 405, body: "Method Not Allowed" };
  }

  if (!event.body) {
    return { statusCode: 400, body: "Missing Data" };
  }

  if (!envVars.KONTENT_SECRET || !envVars.ALGOLIA_API_KEY) {
    return {
      statusCode: 500,
      body: `${missingEnvVars.join(", ")} environment variables are missing, please check the documentation`,
    };
  }

  // Consistency check - make sure your netlify environment variable and your webhook secret matches
  const signatureHelper = new SignatureHelper();
  if (
    !event.headers[signatureHeaderName]
    || !signatureHelper.isValidSignatureFromString(
      event.body,
      envVars.KONTENT_SECRET,
      event.headers[signatureHeaderName],
    )
  ) {
    return { statusCode: 401, body: "Unauthorized" };
  }

  const webhookData: WebhookResponse = JSON.parse(event.body);

  const queryParams = event.queryStringParameters;
  if (!areValidQueryParams(queryParams)) {
    return { statusCode: 400, body: "Missing some query parameters, please check the documentation" };
  }

  const algoliaClient = createAlgoliaClient(queryParams.appId, envVars.ALGOLIA_API_KEY, { userAgent: customUserAgent });
  const index = algoliaClient.initIndex(queryParams.index);

  const actions = (await Promise.all(
    webhookData.notifications
      .filter(n => n.message.object_type === "content_item")
      .map(async notification => {
        const deliverClient = new DeliveryClient({
          projectId: notification.message.environment_id,
          globalHeaders: () => sdkHeaders,
        });

        if (isItemNotification(notification)) {
          return await updateItem({
            index,
            deliverClient,
            slug: queryParams.slug,
            item: notification.data.system,
          });
        }
        return [];
      }),
  )).flat();

  const recordsToReIndex = [
    ...new Map(actions.flatMap(a => a.recordsToReindex.map(i => [i.codename, i] as const))).values(),
  ];
  const objectIdsToRemove = [...new Set(actions.flatMap(a => a.objectIdsToRemove))];

  const reIndexResponse = recordsToReIndex.length ? await index.saveObjects(recordsToReIndex).wait() : undefined;
  const deletedResponse = objectIdsToRemove.length ? await index.deleteObjects(objectIdsToRemove).wait() : undefined;

  return {
    statusCode: 200,
    body: JSON.stringify({
      deletedObjectIds: deletedResponse?.objectIDs ?? [],
      reIndexedObjectIds: reIndexResponse?.objectIDs ?? [],
    }),
    contentType: "application/json",
  };
});

type UpdateItemParams = Readonly<{
  index: SearchIndex;
  deliverClient: DeliveryClient;
  slug: string;
  item: WebhookItemNotification["data"]["system"];
}>;

const updateItem = async (params: UpdateItemParams) => {
  const existingAlgoliaItems = await findAgoliaItems(params.index, params.item.id, params.item.language);

  if (!existingAlgoliaItems.length) {
    const deliverItems = await findDeliverItemWithChildrenByCodename(
      params.deliverClient,
      params.item.codename,
      params.item.language,
    );
    const deliverItem = deliverItems.get(params.item.codename);

    return [{
      objectIdsToRemove: [],
      recordsToReindex: deliverItem && canConvertToAlgoliaItem(params.slug)(deliverItem)
        ? [convertToAlgoliaItem(deliverItems, params.slug)(deliverItem)]
        : [],
    }];
  }

  return Promise.all(existingAlgoliaItems
    .map(async i => {
      const deliverItems = await findDeliverItemWithChildrenByCodename(params.deliverClient, i.codename, i.language);
      const deliverItem = deliverItems.get(i.codename);

      return deliverItem
        ? {
          objectIdsToRemove: [] as string[],
          recordsToReindex: [convertToAlgoliaItem(deliverItems, params.slug)(deliverItem)],
        }
        : { objectIdsToRemove: [i.objectID], recordsToReindex: [] };
    }));
};

const findAgoliaItems = async (index: SearchIndex, itemId: string, languageCodename: string) => {
  try {
    const response = await index.search<AlgoliaItem>("", {
      facetFilters: [`content.id: ${itemId}`, `language: ${languageCodename}`],
    });

    return response.hits;
  } catch {
    return [];
  }
};

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

type ExpectedQueryParams = Readonly<{
  slug: string;
  appId: string;
  index: string;
}>;

const areValidQueryParams = (v: Record<string, unknown> | null): v is ExpectedQueryParams =>
  v !== null
  && hasStringProperty(nameOf<ExpectedQueryParams>("slug"), v)
  && hasStringProperty(nameOf<ExpectedQueryParams>("appId"), v)
  && hasStringProperty(nameOf<ExpectedQueryParams>("index"), v);

const isItemNotification = (notification: WebhookNotification): notification is WebhookItemNotification =>
  notification.message.object_type === "content_item";
