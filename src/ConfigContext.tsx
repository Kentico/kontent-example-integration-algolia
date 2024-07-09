import React, { FC, ReactElement, useEffect, useState } from "react";

import { Config } from "./shared/types/config";
import { findMissingStringProps } from "./shared/utils/findMissingStringProps";

type Props = Readonly<{
  children: ReactElement | ReactElement[] | null;
}>;

export const ConfigProvider: FC<Props> = props => {
  const [config, setConfig] = useState<Config | null>(null);

  useEffect(() => {
    CustomElement.init((element, context) => {
      if (!isValidConfig(element.config)) {
        throw new Error(
          `Invalid element config, the following properties are missing or invalid ${
            findMissingStringProps(Object.keys(emptyConfig))(element.config).join(", ")
          }`,
        );
      }

      setConfig({
        ...element.config,
        projectId: context.projectId,
        language: context.variant.codename,
      });
    });
  }, []);

  if (!config) {
    return null;
  }

  return (
    <ConfigContext.Provider value={config}>
      {props.children}
    </ConfigContext.Provider>
  );
};

ConfigProvider.displayName = "ConfigProvider";

const emptyConfig: Config = {
  slugCodename: "",
  algoliaIndexName: "",
  algoliaSearchKey: "",
  algoliaAppId: "",
  language: "",
  projectId: "",
};

export const ConfigContext = React.createContext<Config>(emptyConfig);

const isValidConfig = (c: Readonly<Record<string, unknown>> | null): c is Config =>
  !findMissingStringProps(Object.keys(emptyConfig))(c).length;
