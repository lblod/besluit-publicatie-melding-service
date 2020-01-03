import request from 'request-promise-native';
import { getExtractedResourceDetailsFromPublishedResource } from './queries';

const PUBLISHER_URI = process.env.PUBLISHER_URI || "http://data.lblod.info/vendors/gelinkt-notuleren";
const KEY = process.env.KEY;
const SOURCE_HOST = process.env.SOURCE_HOST;
const ENDPOINT = process.env.SUBMISSION_ENDPOINT;

if(!SOURCE_HOST) throw 'Please provide SOURCE_HOST';
if(!ENDPOINT) throw 'Please provide ENDPOINT';
if(!KEY) throw 'Please provide KEY';

async function executeSubmitTask(task){
  const publishedResourcesDetail = await getExtractedResourceDetailsFromPublishedResource(task.involves);

  //Note: Probably, I should allow only one extracted resource from a publishedResource
  for(const prDetail of publishedResourcesDetail){
    let payload = createPayloadToSubmit(prDetail.extractedResource,
                                        prDetail.zittingId,
                                        prDetail.bestuurseenheid,
                                        prDetail.bestuurseenheidLabel,
                                        prDetail.classificatieLabel);
    await submitResource(payload);
  }
}

function createPayloadToSubmit(extractedResource, zittingId, bestuurseenheid, bestuurseenheidLabel, classificatieLabel){
  const href = SOURCE_HOST + `/${bestuurseenheidLabel}/${classificatieLabel}/${zittingId}`; //TODO: this is brittle
  return {
    href,
    organization: bestuurseenheid,
    publisher: { uri: PUBLISHER_URI, key: KEY},
    submittedResource: extractedResource
  };
}

async function submitResource(payload){
  const options = {
    method: 'POST',
    uri: ENDPOINT,
    body: payload,
    json: true
  };

  const response = await request(options);
  console.log(response);
  return response;
}

export { executeSubmitTask }
