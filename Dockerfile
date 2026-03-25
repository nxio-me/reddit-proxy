FROM node:22-alpine
WORKDIR /app
COPY index.js .
EXPOSE 3000
USER node
CMD ["node", "index.js"]
