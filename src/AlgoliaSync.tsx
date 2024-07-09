import { FC, useContext, useState } from "react";

import { ConfigContext } from "./ConfigContext";
import { InitRequestBody } from "./shared/types/initRequestBody";

const initFunctionUrl = "/.netlify/functions/algolia-init-function";

enum SynchronizationStatus {
  NotStarted = "NotStarted",
  InProgress = "InProgress",
  Successful = "Successful",
  Failed = "Failed",
}

type Props = Readonly<{
  onSyncDone: () => void;
}>;

export const AlgoliaSync: FC<Props> = props => {
  const config = useContext(ConfigContext);
  const [synchronizationStatus, setSynchronizationStatus] = useState(SynchronizationStatus.NotStarted);

  const syncSearch = () => {
    const body = JSON.stringify(
      {
        projectId: config.projectId,
        language: config.language,
        slugCodename: config.slugCodename,
        algoliaAppId: config.algoliaAppId,
        algoliaIndexName: config.algoliaIndexName,
      } satisfies InitRequestBody,
    );
    setSynchronizationStatus(SynchronizationStatus.InProgress);
    fetch(initFunctionUrl, { method: "POST", body })
      .then(() => {
        setSynchronizationStatus(SynchronizationStatus.Successful);
        props.onSyncDone();
      })
      .catch(e => {
        console.error("Failed to synchronize content, error: ", e);
        setSynchronizationStatus(SynchronizationStatus.Failed);
      });
  };

  return (
    <section className="sync-section">
      <button
        className="btn btn--primary sync-btn"
        disabled={synchronizationStatus === SynchronizationStatus.InProgress}
        onClick={syncSearch}
      >
        Create/Update Search Index for {config.language} language
      </button>
      {renderStatusMessage(synchronizationStatus)}
    </section>
  );
};

AlgoliaSync.displayName = "AlgoliaSync";

const renderStatusMessage = (status: SynchronizationStatus) => {
  switch (status) {
    case SynchronizationStatus.Failed:
      return (
        <div className="status status--validation-failed">
          Synchronization failed. Check the console for more information.
        </div>
      );
    case SynchronizationStatus.InProgress:
      return (
        <div className="status status--in-progress">
          <i className="icon-spinner dropdown-option__icon--loading" /> Synchronizing
        </div>
      );
    case SynchronizationStatus.NotStarted:
      return null;
    case SynchronizationStatus.Successful:
      return (
        <div className="status status--is-ready">
          Content successfully synchronized.
        </div>
      );
    default:
      throw new Error(`Unknown synchronization status: "${status}".`);
  }
};
