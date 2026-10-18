# Build context is the repository root (the image also serves ui/).
FROM node:22-slim AS build
WORKDIR /app/api
COPY api/package.json ./
RUN npm install
COPY api/tsconfig.json ./
COPY api/src ./src
COPY api/test ./test
COPY api/scripts ./scripts
RUN npm run build && npm prune --omit=dev

FROM node:22-slim
WORKDIR /app/api
ENV NODE_ENV=production UI_DIR=/app/ui STORAGE_ROOT=/data
COPY --from=build /app/api/node_modules ./node_modules
COPY --from=build /app/api/dist ./dist
COPY api/package.json ./
COPY ui /app/ui
EXPOSE 3000
CMD ["node", "dist/src/main.js"]
