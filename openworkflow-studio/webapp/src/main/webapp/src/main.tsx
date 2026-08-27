import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { initialize } from "./runtime";
import { applyThemeTokens } from "./theme";
import "./styles.css";

// Must run before the first render - styles.css only ever references
// var(--token), so nothing has a color until this writes the custom
// properties onto <html>.
applyThemeTokens();

const root = createRoot(document.getElementById("root")!);
root.render(<div className="boot">Opening OpenWorkflow Studio…</div>);

initialize()
  .then((identity) =>
    root.render(
      <StrictMode>
        <App identity={identity} />
      </StrictMode>,
    ),
  )
  .catch((failure) =>
    root.render(
      <main className="fatal" role="alert">
        <p>OpenWorkflow Studio</p>
        <h1>Unable To Start This View</h1>
        <pre>
          {failure instanceof Error ? failure.message : String(failure)}
        </pre>
      </main>,
    ),
  );
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
