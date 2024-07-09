import { Hit, HitAttributeHighlightResult } from "instantsearch.js";
import { FC } from "react";
import { Highlight } from "react-instantsearch";

import { AlgoliaItem } from "./functions/utils/algoliaItem";

type Props = Readonly<{
  hit: Hit<AlgoliaItem>;
}>;

export const HitRow: FC<Props> = props => {
  const content = props.hit.content
    .map(c => c.contents)
    .join(" ");
  const contentHighlight = (props.hit._highlightResult?.content as Record<string, any> | null)?.map((v: any) =>
    v.contents
  ) as HitAttributeHighlightResult[] | null;
  const aggregatedContentHighlight: HitAttributeHighlightResult = {
    matchLevel: aggregateMatchLevel(contentHighlight ?? []),
    fullyHighlighted: contentHighlight?.every(r => r.fullyHighlighted),
    value: contentHighlight?.map(r => r.value).join(" ") ?? "",
    matchedWords: [...new Set(contentHighlight?.flatMap(v => v.matchedWords) ?? [])],
  };

  const hitWithModifiedContent = {
    ...props.hit,
    content,
    _highlightResult: { ...props.hit._highlightResult, content: aggregatedContentHighlight },
  };

  return (
    <tr className="hit">
      <td className="hit__name-cell">
        <Highlight
          hit={props.hit}
          attribute="name"
        />
      </td>
      <td className="hit__content-cell">
        <div className="hit__content">
          <Highlight
            hit={hitWithModifiedContent}
            attribute="content"
          />
        </div>
      </td>
    </tr>
  );
};

HitRow.displayName = "HitRow";

const aggregateMatchLevel = (results: HitAttributeHighlightResult[]): HitAttributeHighlightResult["matchLevel"] =>
  results.reduce<HitAttributeHighlightResult["matchLevel"]>((prev, result) => {
    switch (result.matchLevel) {
      case "full":
        return "full";
      case "partial":
        return prev !== "full" ? "partial" : prev;
      default:
        return prev;
    }
  }, "none");
