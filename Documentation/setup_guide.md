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
gcloud services enable --project $KK_PROJECT cloudbuild.googleapis.com
```


## 3. Setup Firebase project

3.1. Go to `console.firebase.google.com`

3.2. Click add project

3.3. Select the Google Cloud project you created earlier

3.4. Select 'pay as you go' plan

3.5. Google Analytics is optional for Katikati projects, it's up to you whether to include it or not

3.6. Click 'Add Firebase' and wait for the indicator that Firebase is ready.
3.6 Click on 'Cloud Firestore' and 'Create database', 'start in production mode', set the location to be a 'multi-region' as close to where your users of Nook will be. We typically we use 'eur3 (europe-west)'

3.7 Click 'Enable'


## 4. Sync data from RapidPro -> Firebase

4.1. Clone https://github.com/larksystems/nook-open-infrastructure/blob/master/setup/rapidpro_sync_token
updating the time to the start of the project

4.2. Navigate to nook-open-infrastructure/sms_connector

4.3. run 
```pipenv --three
pipenv update
pipenv shell

python pubsub_handler_cli.py ~/local_crypto_tokens/$KK_PROJECT.json

```

and in another terminal window run

```
pipenv shell
python rapidpro_adapter_cli.py --crypto-token-file ~/local_crypto_tokens/$KK_PROJECT.json --project-name $KK_PROJECT --credentials-bucket-name $KK_PROJECT-rapidpro-credentials --last-update-token-path ~/GitRepos/Lark/nook-open-infrastructure/setup/rapidpro_sync_token

```

## 5. Setup Nook deployment configuration

5.1. Clone the Nook repo: https://github.com/larksystems/nook

5.2. Clone https://github.com/larksystems/nook-open-infrastructure/blob/master/setup/firebase_constants.json to the location of this folder https://github.com/larksystems/nook/tree/master/webapp/web/assets on your disk

5.3. Log in to https://console.firebase.google.com/

5.4. Add the support email if needed

5.5. Add a web project to the firebase instance

5.6. Setup authentication to use the Google Authentication provider (Firebase Console -> Authentication - Sign In method - Google - Enable)

5.7. Switch to the config view in the firebase SDK and copy the listed contents into the file that you cloned in step (5.2). Note that the configuration in firebase needs the key-names double-quoting in order to be valid JSON

5.8. Navigate to https://github.com/larksystems/nook/tree/master/tool

5.9. Run `./deploy_webapp.sh ../webapp/web/assets/firebase_constants.json ~/local_crypto_tokens/$KK_PROJECT.json`

5.10. Check that Nook is now serving correctly by visiting the line associated with 'authDomain' in the config file. You should be able to log in, howerver when you do you'll reach a page that says that you don't have permission to access the dataset.

## 6. Initialise Nook (TODO Replace with the configurator)

6.1. Add yourself as a user to Nook (TODO: Replace this with the configurator) (Firebase Console -> Firestore -> Add collection (users) -> add document (ID == your email address)

6.2. Setup the shard map for the conversations (Firebase Console -> Firestore -> nook_conversation_shards -> shard-0 -> Add field 'num_shards' : 1 (number)

You should now be able to load Nook and see the conversations!


