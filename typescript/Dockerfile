FROM node:lts-alpine

RUN mkdir -p /home/node/app && chown -R node:node /home/node/app

WORKDIR /home/node/app

COPY package.json ./

RUN npm install

USER node

COPY --chown=node:node . .

RUN npm run build

CMD [ "node", "lib/index.js" ]
