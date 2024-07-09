import { FC, useCallback, useEffect, useRef } from "react";
import { useInfiniteHits, useInstantSearch, usePagination, useSearchBox } from "react-instantsearch";

import { AlgoliaItem } from "./functions/utils/algoliaItem";
import { HitRow } from "./HitRow";

type Props = Readonly<{
  searchIndexKey: string;
}>;

export const AlgoliaSearch: FC<Props> = props => {
  const { hits, showMore, isLastPage } = useInfiniteHits<AlgoliaItem>();
  const { query, refine, clear } = useSearchBox();
  const inputRef = useRef<HTMLInputElement | null>(null);
  const { nbHits } = usePagination();
  const { refresh, status } = useInstantSearch();

  const updateSize = useCallback(() => {
    const newSize = Math.max(document.documentElement.offsetHeight, 200);

    CustomElement.setHeight(Math.ceil(newSize));
  }, []);

  useEffect(() => {
    updateSize();
  }, [hits, updateSize]);

  useEffect(() => {
    refresh();
  }, [props.searchIndexKey, refresh]);

  const clearInput = () => {
    clear();
    inputRef.current?.focus();
  };

  return (
    <>
      <div className="text-field text-field--has-button search-field">
        <input
          ref={inputRef}
          className="text-field__input"
          value={query}
          onChange={e => refine(e.target.value)}
          disabled={status === "stalled"}
        />
        <div
          className="text-field__button-pane"
          onClick={clearInput}
        >
          <i className="text-field__button-icon icon-times-circle" />
        </div>
      </div>
      <b className="hits-number-message">
        {createResultsMessage(hits, nbHits)}
      </b>
      <table className="hits-table">
        <tbody>
          {hits.map(h => (
            <HitRow
              key={h.id}
              hit={h}
            />
          ))}
        </tbody>
      </table>
      {!isLastPage && (
        <div className="show-more-btn-wrapper">
          <button
            className="btn btn--primary"
            onClick={showMore}
          >
            Show more
          </button>
        </div>
      )}
    </>
  );
};

AlgoliaSearch.displayName = "AlgoliaSearch";

const createResultsMessage = (hits: ReadonlyArray<unknown>, totalHits: number): string => {
  if (hits.length === totalHits) {
    return `Found ${totalHits} items.`;
  }
  return `Showing ${hits.length} out of ${totalHits} found items.`;
};
