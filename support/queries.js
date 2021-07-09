import mu from 'mu';
import {uuid, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeInt, sparqlEscapeDate, sparqlEscapeDateTime, sparqlEscapeBool } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import flatten from 'lodash.flatten';
import uniq from 'lodash.uniq';

const DEFAULT_GRAPH = (process.env || {}).DEFAULT_GRAPH || 'http://mu.semte.ch/graphs/public';
const PENDING_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/ongoing";
const FAILED_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/failure";
const SUCCESS_STATUS = "http://lblod.data.gift/besluit-publicatie-melding-statuses/success";
const ADMIN_BODIES_W_DECISION_LIST_THAT_MUST_BE_SUBMITTED = [
  '<http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000005>', // gemeenteraad
  '<http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e00000c>', // provincieraad
  '<http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e000007>', // Raad voor Maatschappelijk Welzijn
  '<http://data.vlaanderen.be/id/concept/BestuursorgaanClassificatieCode/5ab0e9b8a3b2ca7c5e00000a>' // Districtsraad

];
const BESLUIT_TYPES_MELDING = [
  '<https://data.vlaanderen.be/id/concept/BesluitType/0d1278af-b69e-4152-a418-ec5cfd1c7d0b>', // Aanvullend reglement op het wegverkeer m.b.t. gemeentewegen in speciale beschermingszones
  '<https://data.vlaanderen.be/id/concept/BesluitType/1105564e-30c7-4371-a864-6b7329cdae6f>', // Oprichting IGS
  '<https://data.vlaanderen.be/id/concept/BesluitType/256bd04a-b74b-4f2a-8f5d-14dda4765af9>', // Tijdelijke politieverordening (op het wegverkeer)
  '<https://data.vlaanderen.be/id/concept/BesluitType/25deb453-ae3e-4d40-8027-36cdb48ab738>', // Deontologische Code
  '<https://data.vlaanderen.be/id/concept/BesluitType/2f189152-1786-4b55-a3a9-d7f06de63f1c>', // Meerjarenplan(aanpassing) BBC2020
  '<https://data.vlaanderen.be/id/concept/BesluitType/73d5d79f-3f70-4ca9-9512-e216300cd3ac>', // Vaststelling van meerjarenplan BBC2020
  '<https://data.vlaanderen.be/id/concept/BesluitType/1869e152-e724-4dd7-927c-a11e7d832858>', // Vaststelling van meerjarenplanaanpassing BBC2020
  '<https://data.vlaanderen.be/id/concept/BesluitType/380674ee-0894-4c41-bcc1-9deaeb9d464c>', // Oprichting districtsbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/3bba9f10-faff-49a6-acaa-85af7f2199a3>', // Aanvullend reglement op het wegverkeer m.b.t. gemeentewegen in havengebied
  '<https://data.vlaanderen.be/id/concept/BesluitType/3fcf7dba-2e5b-4955-a489-6dd8285c013b>', // Besluit over meerjarenplan(aanpassing) eredienstbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/8d8a75bf-f639-44ae-bcce-50b8f760cc3c>', // Besluit over vaststelling van meerjarenplan eredienstbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/00527d30-e60b-4fa5-9152-f42dddd10ff6>', // Besluit over vaststelling meerjarenplanaanpassing eredienstbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/40831a2c-771d-4b41-9720-0399998f1873>', // Budget
  '<https://data.vlaanderen.be/id/concept/BesluitType/4350cdda-8291-4055-9026-5c7429357fce>', // Advies jaarrekening OCMW-vereniging
  '<https://data.vlaanderen.be/id/concept/BesluitType/70ae4d36-de0c-425d-9dbe-3b6deef8343c>', // Besluit over budget(wijziging) OCMW-vereniging
  '<https://data.vlaanderen.be/id/concept/BesluitType/4673d472-8dbc-4cea-b3ab-f92df3807eb3>', // Personeelsreglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/4d8f678a-6fa4-4d5f-a2a1-80974e43bf34>', // Aanvullend reglement op het wegverkeer enkel m.b.t. gemeentewegen (niet in havengebied of speciale beschermingszones)
  '<https://data.vlaanderen.be/id/concept/BesluitType/5bedea99-905b-4bae-b0d2-e7755c72a5be>', // Besluit over budget AGB
  '<https://data.vlaanderen.be/id/concept/BesluitType/f276bc3b-2d79-4c55-81b8-092e05619676>', // Besluit over meerjarenplan(aanpassing) AGB
  '<https://data.vlaanderen.be/id/concept/BesluitType/bdb04f43-97d2-4be2-aa92-8affd1f3fec8>', // Besluit over vaststelling van meerjarenplan AGB
  '<https://data.vlaanderen.be/id/concept/BesluitType/35d9d01c-015c-4fb9-8ec1-8227f9f4d28c>', // Besluit over vaststelling van meerjarenplanaanpassing AGB
  '<https://data.vlaanderen.be/id/concept/BesluitType/5ee63f84-2fa0-4758-8820-99dca2bdce7c>', // Delegatiereglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/67378dd0-5413-474b-8996-d992ef81637a>', // Reglementen en verordeningen
  '<https://data.vlaanderen.be/id/concept/BesluitType/6af621e2-c807-479e-a6f2-2d64d8339491>', // Goedkeuringstoezicht Voeren
  '<https://data.vlaanderen.be/id/concept/BesluitType/f1d16307-164c-4278-87b9-1d3b1d968f67>', // Statutenwijziging vereniging of vennootschap voor maatschappelijk welzijn
  '<https://data.vlaanderen.be/id/concept/BesluitType/51982214-0d8b-4cd9-87cf-c46570cd1ed3>', // Advies jaarrekening vereniging of vennootschap voor maatschappelijk welzijn
  '<https://data.vlaanderen.be/id/concept/BesluitType/79414af4-4f57-4ca3-aaa4-f8f1e015e71c>', // Advies bij jaarrekening eredienstbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/7d95fd2e-3cc9-4a4c-a58e-0fbc408c2f9b>', // Aanvullend reglement op het wegverkeer m.b.t. één of meerdere gewestwegen
  '<https://data.vlaanderen.be/id/concept/BesluitType/82d0696e-1225-4684-826a-923b2453f5e3>', // Besluit over budget APB
  '<https://data.vlaanderen.be/id/concept/BesluitType/c258f7b8-0bcc-481c-923d-b58b15248422>', // Besluit over meerjarenplan(aanpassing) APB
  '<https://data.vlaanderen.be/id/concept/BesluitType/c76a04b4-cf41-44e1-b597-94c0b3628357>', // Besluit over vaststelling van meerjarenplan APB
  '<https://data.vlaanderen.be/id/concept/BesluitType/31d500ce-bd0a-4ff3-9337-48194f22f37e>', // Besluit over vaststelling van meerjarenplanaanpassing APB
  '<https://data.vlaanderen.be/id/concept/BesluitType/84121221-4217-40e3-ada2-cd1379b168e1>', // Andere
  '<https://data.vlaanderen.be/id/concept/BesluitType/849c66c2-ba33-4ac1-a693-be48d8ac7bc7>', // Besluit meerjarenplan(aanpassing) AGB
  '<https://data.vlaanderen.be/id/concept/BesluitType/8bdc614a-d2f2-44c0-8cb1-447b1017d312>', // Advies bij jaarrekening APB
  '<https://data.vlaanderen.be/id/concept/BesluitType/9f12dc58-18ba-4a1f-9e7a-cf73d0b4f025>', // Besluit budget AGB
  '<https://data.vlaanderen.be/id/concept/BesluitType/a0a709a7-ac07-4457-8d40-de4aea9b1432>', // Advies bij jaarrekening AGB
  '<https://data.vlaanderen.be/id/concept/BesluitType/a8486fa3-6375-494d-aa48-e34289b87d5b>', // Huishoudelijk reglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/b69c9f18-967c-4feb-90a8-8eea3c8ce46b>', // Oprichting ocmw-vereniging
  '<https://data.vlaanderen.be/id/concept/BesluitType/ba5922c9-cfad-4b2e-b203-36479219ba56>', // Retributiereglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/bd0b0c42-ba5e-4acc-b644-95f6aad904c7>', // Oprichting autonoom bedrijf
  '<https://data.vlaanderen.be/id/concept/BesluitType/c417f3da-a3bd-47c5-84bf-29007323a362>', // Besluit over meerjarenplan APB
  '<https://data.vlaanderen.be/id/concept/BesluitType/c945b531-4742-43fe-af55-b13da6ecc6fe>', // Wijziging autonoom bedrijf
  '<https://data.vlaanderen.be/id/concept/BesluitType/d7060f97-c417-474c-abc6-ef006cb61f41>', // Subsidie, premie, erkenning
  '<https://data.vlaanderen.be/id/concept/BesluitType/d9c3d177-6dc6-4775-8c6a-1055a9cbdcc6>', // Wijziging ocmw-vereniging
  '<https://data.vlaanderen.be/id/concept/BesluitType/dbc58656-b0a5-4e43-8e9e-701acb75f9b0>', // Statutenwijziging IGS
  '<https://data.vlaanderen.be/id/concept/BesluitType/df261490-cc74-4f80-b783-41c35e720b46>', // Besluit over budget(wijziging) eredienstbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/e6425cd1-26f2-4cd8-aa0f-1e9b65619c3a>', // Besluit over vaststelling budget eredienstbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/e2928231-d377-48c7-98b4-3bb2f7de65db>', // Besluit over vaststelling budgetwijziging eredienstbestuur
  '<https://data.vlaanderen.be/id/concept/BesluitType/e27ef237-29de-49b8-be22-4ee2ab2d4e5b>', // Toetreding rechtspersoon
  '<https://data.vlaanderen.be/id/concept/BesluitType/e44c535d-4339-4d15-bdbf-d4be6046de2c>', // Jaarrekening
  '<https://data.vlaanderen.be/id/concept/BesluitType/5226e23d-617d-48b9-9c00-3d679ae88fec>', // Statutenwijziging autonoom bedrijf
  '<https://data.vlaanderen.be/id/concept/BesluitType/f4ba730e-3f12-4c2f-81cf-31922b6da166>', // Oprichting rechtspersoon IGS
  '<https://data.vlaanderen.be/id/concept/BesluitType/9a02d21f-fdc4-455e-8892-c2ae1d33759a>', // Statutenwijziging rechtspersoon IGS
  '<https://data.vlaanderen.be/id/concept/BesluitType/6199a44b-0d6c-407c-833a-73abb104efce>', // Oprichting of toetreding vereniging of vennootschap voor maatschappelijk welzijn
  '<https://data.vlaanderen.be/id/concept/BesluitType/2c2f5e3c-827f-4ad7-906a-176f3bda08ce>', // Oprichting van een vereniging voor maatschappelijk welzijn
  '<https://data.vlaanderen.be/id/concept/BesluitType/a8cdb80d-d409-40ed-bb8b-ddfd0acb28df>', // Toetreding tot een vereniging voor maatschappelijk welzijn
  '<https://data.vlaanderen.be/id/concept/BesluitType/2dca7056-06aa-44f0-b8ca-4e1002cdd119>', // Toetreding tot een vennootschap voor maatschappelijk welzijn
  '<https://data.vlaanderen.be/id/concept/BesluitType/e8aee49e-8762-4db2-acfe-2d5dd3c37619>', // Reglement Onderwijs
  '<https://data.vlaanderen.be/id/concept/BesluitType/e8afe7c5-9640-4db8-8f74-3f023bec3241>', // Politiereglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/efa4ec5a-b006-453f-985f-f986ebae11bc>', // Belastingreglement
  '<https://data.vlaanderen.be/id/concept/BesluitType/b2d0734d-13d0-44b4-9af8-1722933c5288>', // Aanvullende belasting of opcentiem
  '<https://data.vlaanderen.be/id/concept/BesluitType/4c22ef0a-f808-41dd-9c9f-2aff17fd851f>', // Contantbelasting
  '<https://data.vlaanderen.be/id/concept/BesluitType/8597e056-b96d-4213-ad4c-37338f2aaf35>', // Kohierbelasting
  '<https://data.vlaanderen.be/id/concept/BesluitType/f56c645d-b8e1-4066-813d-e213f5bc529f>', // Meerjarenplan(aanpassing)
  '<https://data.vlaanderen.be/id/concept/BesluitType/35c15ea0-d0c3-4ba7-b91f-b1c6264800b1>', // Vaststelling van meerjarenplan
  '<https://data.vlaanderen.be/id/concept/BesluitType/23735395-a487-4f0b-9ffa-f8ee1d3cd84f>', // Vaststelling van meerjarenplanaanpassing
  '<https://data.vlaanderen.be/id/concept/BesluitType/f8c070bd-96e4-43a1-8c6e-532bcd771251>', // Oprichting of deelname EVA
  '<https://data.vlaanderen.be/id/concept/BesluitType/09ac9b9b-2585-4195-8c11-8cf8592de213>', // Oprichting van een EVA
  '<https://data.vlaanderen.be/id/concept/BesluitType/94657ec2-e8a1-411f-bb96-d0ea517d7051>', // Deelname in een EVA
  '<https://data.vlaanderen.be/id/concept/BesluitType/fb21d14b-734b-48f4-bd4e-888163fd08e8>', // Rechtspositieregeling (RPR)
  '<https://data.vlaanderen.be/id/concept/BesluitType/fb92601a-d189-4482-9922-ab0efc6bc935>'   // Gebruikersreglement
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
  // decision list published by a gemeenteraad
  // an extract with a decision of certain type

  let queryStr = `
    PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>

    SELECT DISTINCT ?documentType WHERE {
      {
        ?bestuursorgaanInTijd <http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan> ?bestuursorgaan.
        ?bestuursorgaan besluit:classificatie ?bestuursorgaanClassificatie.
        FILTER ( ?bestuursorgaanClassificatie IN (${ADMIN_BODIES_W_DECISION_LIST_THAT_MUST_BE_SUBMITTED.join(',')}))
        GRAPH ?g {
          ?extractedZitting prov:wasDerivedFrom  ${sparqlEscapeUri(resource)}; a besluit:Zitting.
          ?extractedResource prov:wasDerivedFrom  ${sparqlEscapeUri(resource)}; a ?documentType.
          FILTER(?documentType = <http://mu.semte.ch/vocabularies/ext/Besluitenlijst> )
          ?extractedZitting besluit:isGehoudenDoor ?bestuursorgaanInTijd.
        }
        FILTER(?documentType = <http://mu.semte.ch/vocabularies/ext/Besluitenlijst> )
      }
      UNION
      {
        GRAPH ?g {
          ?extractedResource prov:wasDerivedFrom ${sparqlEscapeUri(resource)}.
          ?extractedResource a ?documentType .
          ?extractedResrouce <http://mu.semte.ch/vocabularies/ext/uittrekselBvap> ?behandling.
          ?behandeling prov:generated ?besluit .
          ?besluit rdf:type ?besluitType .
        }
        FILTER ( ?documentType = <http://mu.semte.ch/vocabularies/ext/Uittreksel> && ?besluitType IN ( ${BESLUIT_TYPES_MELDING.join(', ')} ) )
      }
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
const parseResult = function( result ) {
  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      if(row[key] && row[key].datatype == 'http://www.w3.org/2001/XMLSchema#integer' && row[key].value){
        obj[key] = parseInt(row[key].value);
      }
      else obj[key] = row[key]?row[key].value:undefined;
    });
    return obj;
  });
};

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
         getDecisionFromUittreksel,
         getResourcesWithoutTask,
         PENDING_STATUS, FAILED_STATUS, SUCCESS_STATUS,
         PENDING_SUBMISSION_STATUS, FAILED_SUBMISSION_STATUS, SUCCESS_SUBMISSION_STATUS }
