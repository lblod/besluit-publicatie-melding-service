import mu from 'mu';
import {uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import flatten from 'lodash.flatten';
import uniq from 'lodash.uniq';

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/graphs/public';
const PENDING_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/ongoing";
const FAILED_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/failure";
const SUCCESS_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/success";

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
  return (await getTask(subject))[0];
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
  return  parseResult(result);
}

async function getTasksForRetry(maxAttempts){
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
  return  parseResult(result);
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
  return  parseResult(result);
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
    bindingKeys.forEach((key) => obj[key] = row[key]?row[key].value:undefined);
    return obj;
  });
};

export { createTask,
         getPendingTasks,
         getTasksForRetry,
         updateTask,
         getTask,
         getPublishedResourcesFromDelta,
         getExtractedResourceDetailsFromPublishedResource,
         getPublishedResourcesWithoutAssociatedTask,
         PENDING_STATUS, FAILED_STATUS, SUCCESS_STATUS}
