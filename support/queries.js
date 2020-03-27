import mu from 'mu';
import {uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import flatten from 'lodash.flatten';
import uniq from 'lodash.uniq';

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/graphs/public';
const PENDING_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/ongoing";
const FAILED_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/failure";
const SUCCESS_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/success";

const BESLUIT_TYPES_MELDING = [
  '<https://data.vlaanderen.be/id/concept/BesluitType/67378dd0-5413-474b-8996-d992ef81637a>', // Reglementen en verordeningen
  '<https://data.vlaanderen.be/id/concept/BesluitType/0d1278af-b69e-4152-a418-ec5cfd1c7d0b>', // Aanvullend reglement op het wegverkeer m.b.t. gemeentewegen in speciale beschermingszones
  '<https://data.vlaanderen.be/id/concept/BesluitType/256bd04a-b74b-4f2a-8f5d-14dda4765af9>', // Tijdelijke politieverordening (op het wegverkeer)
  '<https://data.vlaanderen.be/id/concept/BesluitType/25deb453-ae3e-4d40-8027-36cdb48ab738>', // Deontologische Code
  '<https://data.vlaanderen.be/id/concept/BesluitType/3bba9f10-faff-49a6-acaa-85af7f2199a3>', // Aanvullend reglement op het wegverkeer m.b.t. gemeentewegen in havengebied
  '<https://data.vlaanderen.be/id/concept/BesluitType/4673d472-8dbc-4cea-b3ab-f92df3807eb3>', // Personeelsreglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/4d8f678a-6fa4-4d5f-a2a1-80974e43bf34>', // Aanvullend reglement op het wegverkeer enkel m.b.t. gemeentewegen (niet in havengebied of speciale beschermingszones)
  '<https://data.vlaanderen.be/id/concept/BesluitType/5ee63f84-2fa0-4758-8820-99dca2bdce7c>', // Delegatiereglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/7d95fd2e-3cc9-4a4c-a58e-0fbc408c2f9b>', // Aanvullend reglement op het wegverkeer m.b.t. één of meerdere gewestwegen
  '<https://data.vlaanderen.be/id/concept/BesluitType/84121221-4217-40e3-ada2-cd1379b168e1>', // Andere
  '<https://data.vlaanderen.be/id/concept/BesluitType/a8486fa3-6375-494d-aa48-e34289b87d5b>', // Huishoudelijk reglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/ba5922c9-cfad-4b2e-b203-36479219ba56>', // Retributiereglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/d7060f97-c417-474c-abc6-ef006cb61f41>', // Subsidie, premie, erkenning
  '<https://data.vlaanderen.be/id/concept/BesluitType/e8aee49e-8762-4db2-acfe-2d5dd3c37619>', // Reglement Onderwijs
  '<https://data.vlaanderen.be/id/concept/BesluitType/e8afe7c5-9640-4db8-8f74-3f023bec3241>', // Politiereglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/efa4ec5a-b006-453f-985f-f986ebae11bc>', // Belastingreglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/fb92601a-d189-4482-9922-ab0efc6bc935>'  // Gebruikersreglement
];

const PENDING_SUBMISSION_STATUS = "http://lblod.data.gift/publication-submission-statuses/ongoing";
const FAILED_SUBMISSION_STATUS = "http://lblod.data.gift/publication-submission-statuses/failure";
const SUCCESS_SUBMISSION_STATUS = "http://lblod.data.gift/publication-submission-statuses/success";

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

async function requiresMelding(resource){
  let queryStr = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

     SELECT DISTINCT ?documentType WHERE{
       GRAPH ?g {
         ?extractedResource prov:wasDerivedFrom ${sparqlEscapeUri(resource)}.
         ?extractedResource a ?documentType.
         ?behandeling prov:generated ?besluit .
         ?besluit rdf:type ?besluitType .
       }
       GRAPH ?h {
         ${sparqlEscapeUri(resource)} ext:publishesBehandeling ?versionedBehandeling .
         ?versionedBehandeling ext:behandeling ?behandeling .
       }
       FILTER( ?documentType IN ( ext:Notulen, ext:Uittreksel ) )
       FILTER( ?besluitType IN ( ${BESLUIT_TYPES_MELDING.join(', ')} ) )
    }
  `;
  let res = await query(queryStr);
  res = parseResult(res);
  return res.length > 0 ;
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

/*************************************************************
 * HELPERS
 *************************************************************/

/**
 * convert results of select query to an array of objects.
 * courtesy: Niels Vandekeybus & Felix
 * @method parseResult
 * @return {Array}
 */
const parseResult = function( result ) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      if(row[key].datatype == 'http://www.w3.org/2001/XMLSchema#integer' && row[key].value){
        obj[key] = parseInt(row[key].value);
      }
      else obj[key] = row[key]?row[key].value:undefined;
    });
    return obj;
  });
};

export { createTask,
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
         PENDING_STATUS, FAILED_STATUS, SUCCESS_STATUS,
         PENDING_SUBMISSION_STATUS, FAILED_SUBMISSION_STATUS, SUCCESS_SUBMISSION_STATUS }
