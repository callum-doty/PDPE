# Mapbox Setup Guide

## The Issue

Your heatmap wasn't loading because you were using a placeholder Mapbox access token. The error showed:

```
GET https://api.mapbox.com/styles/v1/mapbox/dark-v10?access_token=pk.eyJ1IjoicHBtLWRlbW8iLCJhIjoiY2xvYWJjZGVmMGFhYjJxbzFxbzFxbyJ9.demo_token_replace_with_real_token 401 (Unauthorized)
```

## Solution Steps

### 1. Get a Mapbox Access Token

1. Go to [https://account.mapbox.com/](https://account.mapbox.com/)
2. Sign up for a free account (or log in if you have one)
3. Navigate to "Access tokens" in your account dashboard
4. **IMPORTANT**: Use your **PUBLIC** token (starts with `pk.`), NOT the secret token (starts with `sk.`)
5. If you don't have a public token, click "Create a token" and make sure it's set as a public token
6. Copy your PUBLIC access token (it must start with `pk.`)

### 2. Update Your .env File

Replace `YOUR_ACTUAL_MAPBOX_TOKEN_HERE` in your `.env` file with your real token:

```bash
REACT_APP_MAPBOX_ACCESS_TOKEN=pk.your_actual_token_here
```

### 3. Restart Your Development Server

After updating the token, restart your React development server:

```bash
npm start
# or
yarn start
```

## Mapbox Free Tier

- Mapbox offers 50,000 free map loads per month
- This should be sufficient for development and small applications
- No credit card required for the free tier

## Security Note

- Never commit your actual Mapbox token to version control
- The `.env` file should be in your `.gitignore` (which it already is)
- For production, use environment variables on your hosting platform

## Testing

Once you've updated the token and restarted the server, your heatmap should load without the 401 error.
