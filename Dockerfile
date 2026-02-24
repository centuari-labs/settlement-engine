# settlement-engine — production image for Dokploy
# Multi-stage build: compile TypeScript, then run with production deps only.

# -----------------------------------------------------------------------------
# Stage 1: Build
# -----------------------------------------------------------------------------
FROM node:24-alpine AS builder

WORKDIR /app

# Install dependencies (including devDependencies for tsc).
COPY package.json package-lock.json ./
RUN npm ci

# Compile TypeScript.
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# -----------------------------------------------------------------------------
# Stage 2: Production
# -----------------------------------------------------------------------------
FROM node:24-alpine AS production

WORKDIR /app

# Install production dependencies only.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copy compiled output from builder.
COPY --from=builder /app/dist ./dist

# Own files so node user can read them.
RUN chown -R node:node /app

USER node

CMD ["node", "dist/index.js"]
    