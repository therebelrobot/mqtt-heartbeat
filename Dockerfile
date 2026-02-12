# Build stage
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json tsconfig.json ./
RUN npm install

COPY src ./src
RUN npm run build

# Runtime stage
FROM node:22-alpine AS runtime
WORKDIR /app

ENV NODE_ENV=production

# Only ship runtime deps + built file
COPY package.json ./
COPY --from=build /app/node_modules /app/node_modules
COPY --from=build /app/dist /app/dist

# Non-root for better hygiene
USER node

CMD ["node", "dist/index.js"]
