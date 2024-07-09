import algoliasearch from "algoliasearch";
import { FC, ReactElement, useContext, useMemo } from "react";
import { Configure, InstantSearch } from "react-instantsearch";

import { ConfigContext } from "./ConfigContext";
import { customUserAgent } from "./shared/algoliaUserAgent";

type Props = Readonly<{
  children: ReactElement;
}>;

export const AlgoliaSearchContext: FC<Props> = props => {
  const config = useContext(ConfigContext);
  const searchClient = useMemo(
    () => algoliasearch(config.algoliaAppId, config.algoliaSearchKey, { userAgent: customUserAgent }),
    [config.algoliaAppId, config.algoliaSearchKey],
  );

  return (
    <InstantSearch
      indexName={config.algoliaIndexName}
      searchClient={searchClient}
    >
      <Configure
        facets={["language"]}
        facetsRefinements={{ language: [config.language] }}
      />
      {props.children}
    </InstantSearch>
  );
};

AlgoliaSearchContext.displayName = "AlgoliaSearchContext";
