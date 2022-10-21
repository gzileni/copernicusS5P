FROM node:18

RUN apt-get update && apt-get -y install cron

# Create app directory
WORKDIR /usr/src/app

COPY package*.json ./

RUN npm install
RUN npm ci --only=production

COPY . .

CMD ["node", "index.js"]