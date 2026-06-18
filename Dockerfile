# syntax=docker/dockerfile:1.7
# settlement-engine — production image for Dokploy
# Multi-stage build: compile TypeScript, then run with production deps only.

# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM node:24-alpine AS builder

RUN corepack enable && corepack prepare pnpm@10.29.2 --activate

WORKDIR /app

# .npmrc maps @centuari-labs scope to GitHub Packages; auth token is supplied
# at install time via a BuildKit secret mount (never baked into any layer).
COPY package.json pnpm-lock.yaml .npmrc ./

RUN --mount=type=secret,id=npmrc,dst=/root/.npmrc \
    pnpm install --frozen-lockfile

# Compile TypeScript.
COPY tsconfig.json ./
COPY src ./src
RUN pnpm run build

# -----------------------------------------------------------------------------
# Stage 2: Production
# -----------------------------------------------------------------------------
FROM node:24-alpine AS production

RUN corepack enable && corepack prepare pnpm@10.29.2 --activate

WORKDIR /app

# Install production dependencies only.
COPY package.json pnpm-lock.yaml .npmrc ./

RUN --mount=type=secret,id=npmrc,dst=/root/.npmrc \
    pnpm install --prod --frozen-lockfile

# Copy compiled output from builder.
COPY --from=builder /app/dist ./dist

# Own files so node user can read them.
RUN chown -R node:node /app

USER node

CMD ["node", "--max-old-space-size=512", "dist/index.js"]
