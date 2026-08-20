FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY --chown=node:node src ./src

ARG QY_BUILD_VERSION=development
ARG QY_VCS_REF=uncommitted
ARG QY_BUILD_DATE=unknown
ARG QY_SOURCE_URL=local:pachongsys
LABEL org.opencontainers.image.title="qy-auth" \
      org.opencontainers.image.version="${QY_BUILD_VERSION}" \
      org.opencontainers.image.revision="${QY_VCS_REF}" \
      org.opencontainers.image.created="${QY_BUILD_DATE}" \
      org.opencontainers.image.source="${QY_SOURCE_URL}"
USER node
EXPOSE 3000
CMD ["npm", "start"]
