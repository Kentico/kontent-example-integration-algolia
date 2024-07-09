module.exports = {
  root: true,
  env: { browser: true, es2020: true },
  extends: [
    "@kontent-ai",
    "@kontent-ai/eslint-config/react",
  ],
  ignorePatterns: ["dist", ".eslintrc.cjs"],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    project: [
      "./tsconfig.json",
      "./tsconfig.node.json",
      "./src/functions/tsconfig.json",
    ],
  },
  plugins: ["react-refresh"],
  rules: {
    "react-refresh/only-export-components": [
      "warn",
      { allowConstantExport: true },
    ],
  },
};
