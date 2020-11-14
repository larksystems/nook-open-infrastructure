# Setup Guide

## 1. Setup RadidPro API key

1.1. Create a new RapidPro instance (we recomend using TextIt.in)

1.2. Setup the channels that you want to use to send and recieve SMSs

1.3. Locate the API key (click on the org, then about half way down the administrator UI)

1.4. Make a copy of: https://github.com/larksystems/nook-open-infrastructure/blob/master/setup/rapidpro_config.json replacing the API key with the one you've just downloaded from RapidPro, keep a note of the path

## 2. Setup a Google Cloud project

2.1. Register at cloud.goole.com, including enabling billing.

2.2. Install the GCloud and Gsutil tools

2.3.
Run the following on a bash-like terminal replacing the text as appropriate to create the authentication setup

```
export KK_PROJECT="__NAME_OF_YOUR_GCLOUD_PROJECT__"

gsutil -mb -p $KK_PROJECT gs://$KK_PROJECT-rapidpro-credentials
gsutil cp __PATH_TO_THE_CONFIG_FILE__ gs://$KK_PROJECT-rapidpro-credentials
```

2.4 Create the service account (this will store the crypto token fro the service account in ~/local_crypto_tokens, if you want it to be somewhere else, replace the string below and keep a note of where, you'll need it later)

```
gcloud iam service-accounts create $KK_PROJECT --project=$KK_PROJECT --description="katikati-open service account" --display-name="Katikati Open SA"

mkdir ~/local_crypto_tokens
gcloud iam service-accounts keys create ~/local_crypto_tokens/$KK_PROJECT.json --iam-account $KK_PROJECT@$KK_PROJECT.iam.gserviceaccount.com

```

2.5 Assign rights for the service account

```
gcloud projects add-iam-policy-binding $KK_PROJECT --member="serviceAccount:$KK_PROJECT@$KK_PROJECT.iam.gserviceaccount.com" --role="roles/firebase.admin"

gcloud projects add-iam-policy-binding $KK_PROJECT --member="serviceAccount:$KK_PROJECT@$KK_PROJECT.iam.gserviceaccount.com" --role="roles/pubsub.admin"

```

2.6 Enable APIs for the project

```
gcloud services enable --project $KK_PROJECT pubsub.googleapis.com
```
