# ProjectPrint Tradovate Railway Listener

This always-on worker maintains the Tradovate WebSocket connection and forwards fills to ProjectPrint.

## Required variables

```env
PROJECTPRINT_BASE_URL=https://projectprinthq.com
TRADOVATE_LISTENER_SECRET=the-exact-same-value-as-vercel
```

## Start command

```bash
npm start
```

## Railway CLI deployment

```bash
npm i -g @railway/cli
railway login
railway init --name projectprint-tradovate-listener
railway variable set PROJECTPRINT_BASE_URL=https://projectprinthq.com
railway variable set TRADOVATE_LISTENER_SECRET=YOUR_SECRET
railway up
```

No public domain, database, volume, or healthcheck is required. Keep one running replica.
