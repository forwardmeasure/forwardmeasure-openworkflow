import { createTheme, type Theme } from "@mui/material/styles";

// Every color, radius, and font that affects Studio's look and feel lives in
// this one file - swapping the visual theme later (once a "modern agentic
// UI" theme is picked) means replacing THEMES below, not hunting through
// styles.css or component sx props. Plain CSS (styles.css) consumes these
// same values as CSS custom properties written by applyThemeTokens(); MUI
// components (the canvas view) consume them via createStudioMuiTheme().
// Naming mirrors Platform Dashboard's own token set (--panel, --muted,
// --line, --accent, ...) so the two apps' themes stay easy to compare and a
// shared token file can be lifted out later if they ever want one.
export type ThemeTokens = {
  bg: string;
  bgAccent: string;
  panel: string;
  panelBorder: string;
  surfaceRow: string;
  surfaceRowHover: string;
  codeBg: string;
  codeBorder: string;
  chipBg: string;
  chipText: string;
  text: string;
  muted: string;
  line: string;
  accent: string;
  accentHover: string;
  accentContrast: string;
  secondaryBg: string;
  secondaryText: string;
  danger: string;
  dangerContrast: string;
  dangerText: string;
  warning: string;
  success: string;
  focusRing: string;
  radiusSm: string;
  radiusLg: string;
  fontSans: string;
  fontMono: string;
};

// Studio only ships one palette today (dark) - Platform Dashboard's actual
// dark palette (:root[data-theme="dark"] in its own styles.css), not
// Studio's old bespoke green. Same neutral zinc/blue scheme, same values,
// just renamed onto this token set - "the dashboard theme is better" was
// direct feedback, so this reuses it rather than inventing a third palette.
// Platform Dashboard's light/dark/system toggle is deliberately not carried
// over yet: there is no designed light palette to toggle to here. Adding one
// later is a small follow-up - add a "light" entry here and port Dashboard's
// storedThemePreference()/systemPrefersDark() toggle as-is - not a
// re-architecture, since every consumer already reads through tokens rather
// than literal colors.
export const THEMES: Record<"dark", ThemeTokens> = {
  dark: {
    bg: "#09090b",
    bgAccent: "#111113",
    panel: "#141416",
    panelBorder: "#2a2a2e",
    surfaceRow: "#1c1c1f",
    surfaceRowHover: "#232326",
    codeBg: "#0a0a0b",
    codeBorder: "#2a2a2e",
    chipBg: "#232326",
    chipText: "#c8c8cc",
    text: "#f4f4f5",
    muted: "#a1a1aa",
    line: "#2a2a2e",
    accent: "#60a5fa",
    accentHover: "#93c5fd",
    accentContrast: "#0b1220",
    secondaryBg: "#232326",
    secondaryText: "#e4e4e7",
    danger: "#dc2626",
    dangerContrast: "#fff5f5",
    dangerText: "#f87171",
    warning: "#d97706",
    success: "#16a34a",
    focusRing: "#fbbf24",
    radiusSm: "8px",
    radiusLg: "14px",
    fontSans: "Inter Variable, Inter, ui-sans-serif, system-ui, sans-serif",
    fontMono: "ui-monospace, monospace",
  },
};

const THEME_STORAGE_KEY = "openworkflow-studio-theme";

export function activeThemeName(): keyof typeof THEMES {
  const stored = localStorage.getItem(THEME_STORAGE_KEY);
  return stored && stored in THEMES ? (stored as keyof typeof THEMES) : "dark";
}

// Writes every token as a CSS custom property on <html> - styles.css never
// hardcodes a color/radius/font literal, it only references var(--token).
// Called synchronously before the first render (see main.tsx) so nothing
// ever paints with unset custom properties.
export function applyThemeTokens(
  name: keyof typeof THEMES = activeThemeName(),
): void {
  const tokens = THEMES[name];
  const root = document.documentElement.style;
  for (const [key, value] of Object.entries(tokens)) {
    root.setProperty(`--${camelToKebab(key)}`, value);
  }
  document.documentElement.dataset.theme = name;
  localStorage.setItem(THEME_STORAGE_KEY, name);
}

function camelToKebab(value: string): string {
  return value.replace(/[A-Z]/g, (letter) => `-${letter.toLowerCase()}`);
}

// MUI's palette needs literal color values - it computes hover/contrast
// shades at theme-creation time via color math that can't parse a
// var(--accent) string - so this reads the same THEMES entry directly rather
// than the CSS custom properties applyThemeTokens() writes out.
export function createStudioMuiTheme(
  name: keyof typeof THEMES = activeThemeName(),
): Theme {
  const tokens = THEMES[name];
  return createTheme({
    palette: {
      mode: "dark",
      primary: { main: tokens.accent, contrastText: tokens.accentContrast },
      error: { main: tokens.danger, contrastText: tokens.dangerContrast },
      warning: { main: tokens.warning },
      success: { main: tokens.success },
      background: { default: tokens.bg, paper: tokens.panel },
      divider: tokens.line,
      text: { primary: tokens.text, secondary: tokens.muted },
    },
    shape: { borderRadius: parseInt(tokens.radiusSm, 10) },
    // htmlFontSize matches styles.css's :root font-size: 87.5% (14px, not
    // the browser's unstyled 16px default) - MUI computes every component's
    // rem-based sizing against this value, so leaving it at MUI's own
    // 16px-assuming default here would make Buttons/TextFields/etc. render
    // LARGER than the 14px root they're actually laid out against.
    typography: { fontFamily: tokens.fontSans, htmlFontSize: 14 },
  });
}
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
