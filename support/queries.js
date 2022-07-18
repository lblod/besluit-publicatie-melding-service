import mu from 'mu';
import {uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import flatten from 'lodash.flatten';
import uniq from 'lodash.uniq';
import fetch from 'node-fetch';

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/graphs/public';
const PENDING_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/ongoing";
const FAILED_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/failure";
const SUCCESS_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/success";
const PENDING_SUBMISSION_STATUS = "http://lblod.data.gift/publication-submission-statuses/ongoing";
const FAILED_SUBMISSION_STATUS = "http://lblod.data.gift/publication-submission-statuses/failure";
const SUCCESS_SUBMISSION_STATUS = "http://lblod.data.gift/publication-submission-statuses/success";

const BESLUIT_TYPES_ENDPOINT = (process.env || {}).BESLUIT_TYPES_ENDPOINT || 'https://centrale-vindplaats.lblod.info/sparql';
const BESTUURSORGANEN_NEED_PUBLISHING = [
  'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000005', // gemeenteraad
  'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e00000c', // provincieraad
  'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000007', // Raad voor Maatschappelijk Welzijn
  'http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e00000a', // Districtsraad

];
const DOCUMENT_TYPE_ALIASSES = (new Map())
  .set('https://data.vlaanderen.be/id/concept/BesluitDocumentType/3fa67785-ffdc-4b30-8880-2b99d97b4dee', ['http://mu.semte.ch/vocabularies/ext/Besluitenlijst'])
  .set('https://data.vlaanderen.be/id/concept/BesluitDocumentType/9d5bfaca-bbf2-49dd-a830-769f91a6377b', ['http://mu.semte.ch/vocabularies/ext/Uittreksel'])
  .set('https://data.vlaanderen.be/id/concept/BesluitDocumentType/13fefad6-a9d6-4025-83b5-e4cbee3a8965', ['http://mu.semte.ch/vocabularies/ext/Agenda'])
  .set('https://data.vlaanderen.be/id/concept/BesluitDocumentType/8e791b27-7600-4577-b24e-c7c29e0eb773', ['http://mu.semte.ch/vocabularies/ext/Notulen']);
let REPORTING_DATA_BESLUIT_TYPES = [];
let REPORTING_DATA_BESLUIT_DOCUMENT_TYPES = [];

async function refreshReportingData() {
  console.log('REPORTING DATA REFRESH');
  const makeQuery = (type) => `
    PREFIX conceptscheme: <https://data.vlaanderen.be/id/conceptscheme/>
    PREFIX   borgaancode: <http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/>
    PREFIX   BesluitType: <https://data.vlaanderen.be/id/concept/BesluitType/>
    PREFIX          skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX       besluit: <http://lblod.data.gift/vocabularies/besluit/>
    PREFIX           sch: <http://schema.org/>
    PREFIX          rule: <http://lblod.data.gift/vocabularies/notification/>

    SELECT ?subject ?bestuurseenheidclassificatiecode ?obligationToReport ?validFrom ?validThrough WHERE {
      ?subject skos:inScheme conceptscheme:${type} ;
               besluit:notificationRule ?rule .
      ?rule besluit:decidableBy ?bestuurseenheidclassificatiecode ;
            besluit:obligationToReport ?obligationToReport .
      OPTIONAL { ?rule sch:validFrom ?validFrom . }
      OPTIONAL { ?rule sch:validThrough ?validThrough . }
    }
  `;

  //BesluitTypes
  const besluitTypesParams = new URLSearchParams();
  besluitTypesParams.append('query', makeQuery('BesluitType'));
  const besluitTypesResponse = await fetch(BESLUIT_TYPES_ENDPOINT, {
    method: 'POST',
    body: besluitTypesParams,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/sparql-results+json,application/json',
    },
  });
  const besluitTypesResults = await besluitTypesResponse.json();
  
  //BesluitDocumentTypes
  const besluitDocumentTypesParams = new URLSearchParams();
  besluitDocumentTypesParams.append('query', makeQuery('BesluitDocumentType'));
  const besluitDocumentTypesResponse = await fetch(BESLUIT_TYPES_ENDPOINT, {
    method: 'POST',
    body: besluitDocumentTypesParams,
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Accept': 'application/sparql-results+json,application/json',
    },
  });
  const besluitDocumentTypesResults = await besluitDocumentTypesResponse.json();

  //Save results in global variables
  REPORTING_DATA_BESLUIT_TYPES = parseResult(besluitTypesResults);
  //Document types need aliasses for legacy type information
  REPORTING_DATA_BESLUIT_DOCUMENT_TYPES = addAliasses(parseResult(besluitDocumentTypesResults), 'subject', DOCUMENT_TYPE_ALIASSES);
}

async function getTaskForResource(publishedResource){
  let q = `
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

     SELECT DISTINCT ?task {
       GRAPH ?graph {
         ${sparqlEscapeUri(publishedResource)} a sign:PublishedResource.
         ?task nuao:involves ${sparqlEscapeUri(publishedResource)}.
      }
    }
  `;

  let result = await query(q);
  if(result.results.bindings.length == 0){
    return null;
  }
  result = parseResult(result);
  return (await getTask(result[0].task));
}

async function createTask(publishedResource){
  let sUuid = uuid();
  let subject = `http://lblod.data.gift/besluit-publicatie-melding-events/${sUuid}`;
  let created = Date.now();

  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    dct: <http://purl.org/dc/terms/>

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(subject)} a task:Task;
                                                mu:uuid ${sparqlEscapeString(sUuid)};
                                                adms:status ${sparqlEscapeUri(PENDING_STATUS)};
                                                task:numberOfRetries ${sparqlEscapeInt(0)};
                                                dct:created ${sparqlEscapeDateTime(created)};
                                                dct:modified ${sparqlEscapeDateTime(created)};
                                                dct:creator <http://lblod.data.gift/services/besluit-publicatie-melding-service>;
                                                nuao:involves ${sparqlEscapeUri(publishedResource)}.
      }
    }
  `;

  await query(q);
  return (await getTask(subject));
}

async function getPendingTasks(){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>
    SELECT DISTINCT ?subject ?uuid ?status ?created ?modified ?numberOfRetries ?involves WHERE{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        VALUES ?status { ${sparqlEscapeUri(PENDING_STATUS)} }.
        ?subject a   task:Task;
                     mu:uuid ?uuid;
                     adms:status ?status;
                     task:numberOfRetries ?numberOfRetries;
                     dct:created ?created;
                     dct:modified ?modified;
                     nuao:involves ?involves.
      }
    }
  `;

  let result = await query(q);
  return parseResult(result);
}

async function getFailedTasksForRetry(maxAttempts){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>
    SELECT DISTINCT ?subject ?uuid ?status ?created ?modified ?numberOfRetries ?involves WHERE{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        VALUES ?status { ${sparqlEscapeUri(FAILED_STATUS)} }.
        ?subject a   task:Task;
                     mu:uuid ?uuid;
                     adms:status ?status;
                     task:numberOfRetries ?numberOfRetries;
                     dct:created ?created;
                     dct:modified ?modified;
                     nuao:involves ?involves.

       FILTER( ?numberOfRetries < ${sparqlEscapeInt(maxAttempts)} )
      }
    }
  `;

  let result = await query(q);
  return parseResult(result);
}

async function getTask(subjectUri){
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    mu: <http://mu.semte.ch/vocabularies/core/>
    PREFIX    nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    ndo: <http://oscaf.sourceforge.net/ndo.html#>
    SELECT DISTINCT ?subject ?uuid ?status ?created ?modified ?numberOfRetries ?involves WHERE{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        VALUES ?subject { ${sparqlEscapeUri(subjectUri)} }.
        ?subject a   task:Task;
                     mu:uuid ?uuid;
                     adms:status ?status;
                     task:numberOfRetries ?numberOfRetries;
                     dct:created ?created;
                     dct:modified ?modified;
                     nuao:involves ?involves.
      }
    }
  `;

  let result = await query(q);
  return parseResult(result)[0];
}

async function getPublishedResourcesWithoutAssociatedTask(){
  let queryStr = `
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>

     SELECT DISTINCT ?resource {
       GRAPH ?graph {
         ?resource a sign:PublishedResource.
         ?resource <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status> <http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/success>.

         FILTER NOT EXISTS {
          ?task nuao:involves ?resource.
         }
      }
    }
  `;

  const res = await query(queryStr);
  return parseResult(res);
}

async function getExtractedResourceDetailsFromPublishedResource(resource){
  let queryStr = `
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

     SELECT DISTINCT ?extractedResource ?extractedResourcePredicate ?type ?bestuurseenheid ?bestuurseenheidLabel ?classificatie ?classificatieLabel ?zitting ?zittingId WHERE{

       GRAPH ?graph {
         ?extractedResource prov:wasDerivedFrom ${sparqlEscapeUri(resource)}.
         ?extractedResource a ?type.
         ?zitting ?extractedResourcePredicate ?extractedResource.
         ?zitting besluit:isGehoudenDoor ?bot.
         ?zitting mu:uuid ?zittingId.

         ?bot mandaat:isTijdspecialisatieVan ?bo.
         ?bo besluit:bestuurt ?bestuurseenheid.
         ?bestuurseenheid skos:prefLabel ?bestuurseenheidLabel.
         ?bestuurseenheid besluit:classificatie ?classificatie.
         ?classificatie skos:prefLabel ?classificatieLabel.
       }
       FILTER(
         ?type IN ( ext:Notulen, ext:Agenda, ext:Uittreksel, ext:Besluitenlijst)
       )
    }
  `;
  let res = await query(queryStr);
  return parseResult(res);
}

async function requiresMelding(resource) {
  //Get some information about the published resource
  //TODO: change the publicationDate from startedAtTime to the real publicationDate
  const informationQuery = `
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
    PREFIX mandaat: <http://data.vlaanderen.be/ns/mandaat#>

    SELECT ?documentType ?besluitType ?bestuursorgaanclassificatiecode ?bestuurseenheidclassificatiecode ?publicationDate WHERE {
      ?zitting rdf:type besluit:Zitting ;
               prov:wasDerivedFrom ${sparqlEscapeUri(resource)} ;
               besluit:isGehoudenDoor ?bestuursorgaan .
      ?bestuursorgaan mandaat:isTijdspecialisatieVan ?tijdsspecialisatie .
      ?tijdsspecialisatie besluit:bestuurt / besluit:classificatie ?bestuurseenheidclassificatiecode .
      ?tijdsspecialisatie besluit:classificatie ?bestuursorgaanclassificatiecode .
      ?resource prov:wasDerivedFrom ${sparqlEscapeUri(resource)} ;
                rdf:type ?documentType .
      FILTER (!sameTerm(?zitting, ?resource))
      OPTIONAL {
        ?zitting prov:startedAtTime ?publicationDate .
      }
      OPTIONAL {
        ?resource rdf:type ext:Uittreksel ;
                  ext:uittrekselBvap ?behandelingVanAgendapunt .
        ?behandelingVanAgendapunt prov:generated ?besluit .
        ?besluit rdf:type ?besluitType .
        FILTER (STRSTARTS(STR(?besluitType), "https://data.vlaanderen.be/id/concept/BesluitType/"))
      }
    }
  `;
  const results = await query(informationQuery);
  const informationObject = parseResult(results)[0];
  console.log('RESULTS:', results);
  console.log('INFORMATION ABOUT PUBLISHED RESOURCE', informationObject);

  //Make sure that if no date was found, the date of this moment is taken (should only be minutes after the real publication)
  informationObject.publicationDate = informationObject.publicationDate || new Date();

  //Check if document type needs reporting
  const documentNeedsNotification = REPORTING_DATA_BESLUIT_DOCUMENT_TYPES
    .some((type) => {
      console.log('URI TEST:', type.subject, type.subjectAliasses, informationObject.documentType);
      if (type.subject != informationObject.documentType && !type.subjectAliasses.find((alias) => alias == informationObject.documentType)) return false;
      console.log('CODE TEST:', type.bestuurseenheidclassificatiecode, informationObject.bestuurseenheidclassificatiecode);
      if (type.bestuurseenheidclassificatiecode != informationObject.bestuurseenheidclassificatiecode) return false;
      console.log('DATE TEST:', type.validFrom, type.validThrough, informationObject.publicationDate);
      //No date range exists, this rule is invalid
      if (!(type.validFrom || type.validThrough))
        return false;
      //If rule start is after publication date, this is not a valid rule for the published resource
      if (type.validFrom && type.validFrom > informationObject.publicationDate)
        return false;
      //If rule end is before or on the publication date, this is also not a valid rule
      if (type.validThrough && type.validThrough <= informationObject.publicationDate)
        return false;
      return type.obligationToReport;
    }) &&
    !!(BESTUURSORGANEN_NEED_PUBLISHING.find((orgaancode) => {
      console.log('ORGAAN TEST:', informationObject.bestuursorgaanclassificatiecode, orgaancode);
      return orgaancode == informationObject.bestuursorgaanclassificatiecode
    }));
  console.log('DOCUMENT NEEDS MELDING:', documentNeedsNotification);
  if (documentNeedsNotification) return true;

  //If document does not need reporting, check if this is about a beluitType that needs reporting
  if (informationObject.besluitType) {
    console.log('TESTING IF BESLUIT NEEDS MELDING');
    const besluitNeedsNotification = REPORTING_DATA_BESLUIT_TYPES
      .some((type) => {
        if (type.subject != informationObject.besluitType) return false;
        if (type.bestuurseenheidclassificatiecode != informationObject.bestuurseenheidclassificatiecode) return false;
        if (!(type.validFrom || type.validThrough))
          return false;
        if (type.validFrom && type.validFrom > informationObject.publicationDate)
          return false;
        if (type.validThrough && type.validThrough <= informationObject.publicationDate)
          return false;
        return type.obligationToReport;
      });
    console.log('BESLUIT NEEDS MELDING:', besluitNeedsNotification);
    return besluitNeedsNotification;
  }
  else {
    console.log('NOT A DOCUMENT, NOR BESLUIT THAT NEEDS MELDING');
    return false;
  }
}

async function updateTask(uri, newStatusUri, numberOfRetries){
  let updated = Date.now();
  let q = `
    PREFIX    adms: <http://www.w3.org/ns/adms#>
    PREFIX    dct: <http://purl.org/dc/terms/>
    PREFIX    task: <http://redpencil.data.gift/vocabularies/tasks/>

    DELETE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status;
                                task:numberOfRetries ?numberOfRetries;
                                dct:modified ?modified.
      }
    }
    WHERE {
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} adms:status ?status;
                                task:numberOfRetries ?numberOfRetries;
                                dct:modified ?modified.
      }
    }
    ;
    INSERT DATA{
      GRAPH ${sparqlEscapeUri(DEFAULT_GRAPH)} {
        ${sparqlEscapeUri(uri)} dct:modified ${sparqlEscapeDateTime(updated)};
                                task:numberOfRetries ${sparqlEscapeInt(numberOfRetries)};
                                adms:status ${sparqlEscapeUri(newStatusUri)}.
      }
    }
  `;

  await query(q);
  return getTask(uri);
}

async function updatePublishedResourceStatus(uri, newStatusUri) {
  let q = `
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

    DELETE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} ext:submissionStatus ?submissionStatus .
      }
    } WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} a sign:PublishedResource ;
          ext:submissionStatus ?submissionStatus .
      }
    }
    ;
    INSERT {
      GRAPH ?h {
        ${sparqlEscapeUri(uri)} ext:submissionStatus ${sparqlEscapeUri(newStatusUri)}.
      }
    } WHERE {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} a sign:PublishedResource .
      }
      BIND (?g as ?h)
    }
  `;

  await query(q);
}

function getPublishedResourcesFromDelta(delta) {
  const inserts = flatten(delta.map(changeSet => changeSet.inserts));
  const publishedResourceUris = inserts.filter( triple => {
    return triple.predicate.type == 'uri'
      && triple.predicate.value == 'http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status'
      && triple.object.type == 'uri'
      && triple.object.value == 'http://mu.semte.ch/vocabularies/ext/besluit-publicatie-publish-service/status/success';
  }).map( triple => triple.subject.value );
  return uniq(publishedResourceUris);
}

async function getUuid(uri){
  let queryStr = `
       PREFIX  mu:  <http://mu.semte.ch/vocabularies/core/>
       SELECT DISTINCT ?uuid {
         GRAPH ?g {
            ${sparqlEscapeUri(uri)} mu:uuid ?uuid.
         }
       }
  `;

  let uuid = parseResult(await query(queryStr))[0];
  if (uuid) {
    return uuid.uuid;
  } else {
    return null;
  }
};

async function getDecisionFromUittreksel(uri) {
  let queryStr = `
    PREFIX  mu:  <http://mu.semte.ch/vocabularies/core/>

    SELECT DISTINCT ?decision {
      GRAPH ?g {
        ${sparqlEscapeUri(uri)} <http://mu.semte.ch/vocabularies/ext/uittrekselBvap> ?behandelingVanAgendapunt .
        ?behandelingVanAgendapunt <http://www.w3.org/ns/prov#generated> ?decision .
      }
    }
  `;
  let decision = parseResult(await query(queryStr))[0];
  if (decision) {
    return decision.decision;
  } else {
    return null;
  }
}


/*************************************************************
 * HELPERS
 *************************************************************/

/**
 * convert results of select query to an array of objects.
 * courtesy: Niels Vandekeybus & Felix
 * @method parseResult
 * @return {Array}
 */
function parseResult (result) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      if (row[key] && row[key].datatype) {
        switch (row[key].datatype) {
          case 'http://www.w3.org/2001/XMLSchema#integer':
            obj[key] = parseInt(row[key].value);
            break;
          case 'http://www.w3.org/2001/XMLSchema#date':
          case 'http://www.w3.org/2001/XMLSchema#dateTime':
            obj[key] = new Date(row[key].value);
            break;
          case 'http://www.w3.org/2001/XMLSchema#boolean':
            obj[key] = !!(row[key].value == '1' || row[key].value == 'true');
            break;
          default:
            obj[key] = row[key].value;
            break;
        }
      }
      else obj[key] = row[key] ? row[key].value : undefined;
    });
    return obj;
  });
}

function addAliasses(objects, property, aliasmap) {
  return objects.map((object) => {
    object[`${property}Aliasses`] = aliasmap.get(object[property]) || [];
    return object;
  });
}

async function getResourcesWithoutTask() {
  let queryString = `
    PREFIX sign: <http://mu.semte.ch/vocabularies/ext/signing/>
    PREFIX nuao: <http://www.semanticdesktop.org/ontologies/2010/01/25/nuao#>
    select distinct * where {
      ?resource a sign:PublishedResource.
      FILTER NOT EXISTS {
        ?task nuao:involves ?resource.
      }
    }
  `;
  const result = await query(queryString);
  const parsedResult = parseResult(result);
  return parsedResult;
}

export { refreshReportingData,
         createTask,
         requiresMelding,
         getTaskForResource,
         getPendingTasks,
         getFailedTasksForRetry,
         updateTask,
         updatePublishedResourceStatus,
         getTask,
         getPublishedResourcesFromDelta,
         getExtractedResourceDetailsFromPublishedResource,
         getPublishedResourcesWithoutAssociatedTask,
         getUuid,
         getDecisionFromUittreksel,
         getResourcesWithoutTask,
         PENDING_STATUS, FAILED_STATUS, SUCCESS_STATUS,
         PENDING_SUBMISSION_STATUS, FAILED_SUBMISSION_STATUS, SUCCESS_SUBMISSION_STATUS }
