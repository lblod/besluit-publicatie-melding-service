import fetch from 'node-fetch';
import { getExtractedResourceDetailsFromPublishedResource, getUuid, getDecisionFromUittreksel } from './queries';

const PUBLISHER_URI = process.env.PUBLISHER_URI || "http://data.lblod.info/vendors/gelinkt-notuleren";
const KEY = process.env.KEY;
const SOURCE_HOST = process.env.SOURCE_HOST;
const ENDPOINT = process.env.SUBMISSION_ENDPOINT;

if(!SOURCE_HOST) throw 'Please provide SOURCE_HOST';
if(!ENDPOINT) throw 'Please provide ENDPOINT';
if(!KEY) throw 'Please provide KEY';

const RESOURCE_TO_URL_TYPE_MAP = {
  'http://mu.semte.ch/vocabularies/ext/Agenda': 'agenda',
  'http://mu.semte.ch/vocabularies/ext/Besluitenlijst': 'besluitenlijst',
  'http://mu.semte.ch/vocabularies/ext/Notulen': 'notulen',
  'http://mu.semte.ch/vocabularies/ext/Uittreksel': 'uittreksels'
};

async function executeSubmitTask(task){
  const publishedResourcesDetail = await getExtractedResourceDetailsFromPublishedResource(task.involves);

  //Note: Probably, I should allow only one extracted resource from a publishedResource
  for(const prDetail of publishedResourcesDetail){
    let payload = await createPayloadToSubmit(prDetail.type,
                                        prDetail.extractedResource,
                                        prDetail.zittingId,
                                        prDetail.bestuurseenheid,
                                        prDetail.bestuurseenheidLabel,
                                        prDetail.classificatieLabel);
    const validToSubmit = await hrefReturnsOk(payload.href);
    if (validToSubmit) {
      return await submitResource(payload);
    }
    else {
      throw `preflight check failed: ${payload.href} is not returing a 200 response`;
    }
  }
}

async function hrefReturnsOk(url) {
  try {
    const result = await fetch(url);
    if (result.status === 200) {
      return true;
    }
    else {
      return false;
    }
  }
  catch(e) {
    console.log("error", e);
    return false;
  }


}
async function createPayloadToSubmit(type, extractedResource, zittingId, bestuurseenheid, bestuurseenheidLabel, classificatieLabel) {
  let href = null;

  if (RESOURCE_TO_URL_TYPE_MAP[type] == 'uittreksels') {
    /* Uittreksels need a specific treatment because of the way they are published on the publication app and
      because they are not a decison type in themselves so we need to pass the decision contained in them as payload.
    */
    const uittrekselUuid = await getUuid(extractedResource);
    href = SOURCE_HOST + `/${bestuurseenheidLabel}/${classificatieLabel}/${zittingId}/${RESOURCE_TO_URL_TYPE_MAP[type]}/${uittrekselUuid}`;
    extractedResource = await getDecisionFromUittreksel(extractedResource);
  } else {
    href = SOURCE_HOST + `/${bestuurseenheidLabel}/${classificatieLabel}/${zittingId}/${RESOURCE_TO_URL_TYPE_MAP[type]}`;
  }

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
    body:    JSON.stringify(payload),
    headers: { 'Content-Type': 'application/json' }
  };

  const response = await fetch(ENDPOINT, options);
  return response;
}

export { executeSubmitTask }
