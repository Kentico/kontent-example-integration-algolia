import "./main.css";

import React from "react";
import ReactDOM from "react-dom/client";
import { PoweredBy } from "react-instantsearch";

import { AlgoliaElement } from "./AlgoliaElement";
import { ConfigProvider } from "./ConfigContext";
import { EnsureKontentAsParent } from "./EnsureKontentAsParent";

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement,
);
root.render(
  <React.StrictMode>
    <EnsureKontentAsParent>
      <ConfigProvider>
        <AlgoliaElement />
      </ConfigProvider>
      <PoweredBy />
    </EnsureKontentAsParent>
  </React.StrictMode>,
);
