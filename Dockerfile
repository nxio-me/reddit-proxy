FROM node:22-alpine
RUN apk add --no-cache curl
WORKDIR /app
COPY index.js .
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -sf http://127.0.0.1:3000/ || exit 1
USER node
CMD ["node", "index.js"]
