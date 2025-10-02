import { app, errorHandler } from 'mu';
import {
  PENDING_STATUS,
  FAILED_STATUS,
  SUCCESS_STATUS,
  PENDING_SUBMISSION_STATUS,
  FAILED_SUBMISSION_STATUS,
  SUCCESS_SUBMISSION_STATUS,
  ALREADY_SUBMITED_STATUS,
  ALREADY_SUBMITED_SUBMISSION_STATUS,
  updatePublishedResourceStatus
} from './support/queries.js' ;
import { waitForDatabase } from './database-utils.js';
import { 
  getPendingTasks,
  requiresMelding,
  getTaskForResource,
  getFailedTasksForRetry,
  createTask,
  updateTask,
  updatePublishedResourceStatus,
  getPublishedResourcesFromDelta ,
  getResourcesWithoutTask,
  refreshReportingData
} from './support/queries.js';
import { executeSubmitTask } from './support/pipeline.js';
import bodyParser from 'body-parser';
import { CronJob } from 'cron';
import { Mutex } from 'async-mutex';


const mutex = new Mutex();


const CRON_FREQUENCY = process.env.RESCHEDULE_CRON_PATTERN || '0 0 * * *';
const CRON_DATA_REFRESH = process.env.DATA_REFRESH_CRON_PATTERN || '0 0 5 */1 * *'; //Every day at 5:00
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || 10);

waitForDatabase(rescheduleUnproccessedTasks.bind(this, true));

app.use( bodyParser.json( { type: function(req) { return /^application\/json/.test( req.get('content-type') ); } } ) );

app.get('/', function( req, res ) {
  res.send(`Welcome to besluit-publicatie-melding service`);
});

app.post('/submit-publication', async function( req, res ){
  const delta = req.body;
  const publishedResourceUris = getPublishedResourcesFromDelta(delta);
  console.log(`Found ${publishedResourceUris.length} new remote data objects in the delta message`);
  processPublishedResources(publishedResourceUris);
  res.send({message: `Started.`});
});

app.post('/refresh-reporting-data', async function (req, res) {
  await refreshReportingData();
  res.send({ message: 'Reporting data will be refreshed' });
});

app.get('/test-if-melding', async function (req, res) {
  try {
    const needsMelding = await requiresMelding(req.query.resource);
    res.json({ needsMelding });
  }
  catch (error) {
    console.error(error);
    res.json({ error: 'There seems to have occured an error.', errorobject: error  });
  }
})

app.use(errorHandler);

async function processPublishedResources(publishedResourceUris){
  const release = await mutex.acquire();
  for(const pr of publishedResourceUris){
    try {
      if(!(await requiresMelding(pr))){
        console.log(`No melding required for ${pr}`);
        continue;
      };
  
      let task = await getTaskForResource(pr);
      if(task){
        console.log(`A task already exists for ${pr}, skipping`);
        continue; //We assume this is picked up previously
      }
  
      task = await createTask(pr);
  
      try{
        const response = await executeSubmitTask(task);
        if (response.ok) {
          await updateTask(task.subject, SUCCESS_STATUS, task.numberOfRetries);
          await updatePublishedResourceStatus(task.involves, SUCCESS_SUBMISSION_STATUS);
        } else if (response.status === 409) {
          await updateTask(
            task.subject,
            ALREADY_SUBMITED_STATUS,
            task.numberOfRetries
          );
          await updatePublishedResourceStatus(
            task.involves,
            ALREADY_SUBMITED_SUBMISSION_STATUS
          );
          const responseJson = await response.json();
          await generateAlreadySubmittedLog(responseJson);
        }
        else {
          handleTaskError("error submitting resource ${pr}, status: ${response.statusText}. ${body.text()}", task);
        }
      }
      catch(error){
        handleTaskError(error, task);
      }
    } catch(error){
      console.error('Failed processing published resource: ', pr);
    }
  }
  release();
}

async function handleTaskError(error, task){
  console.error(`Error for task ${task.subject}`);
  console.error(error);
  task = await updateTask(task.subject, FAILED_STATUS, task.numberOfRetries + 1);
  await updatePublishedResourceStatus(task.involves, FAILED_SUBMISSION_STATUS);


  if(task.numberOfRetries >= MAX_ATTEMPTS){
    console.log(`Stopping retries for task ${task.subject})`);
  }
  else scheduleRetryProcessing(task);
}

async function scheduleRetryProcessing(task){
  console.log(`Tried processing ${task.subject} retried ${task.numberOfRetries}/${MAX_ATTEMPTS} already`);

  const waitTime = calcTimeout(task.numberOfRetries);

  console.log(`Expecting next retry for ${task.subject} in about ${waitTime/1000} seconds`);
  setTimeout(async () => {
    try {
      console.log(`Retry for task ${task.subject}`);
      await updateTask(task.subject, PENDING_STATUS, task.numberOfRetries);
      await updatePublishedResourceStatus(task.involves, PENDING_SUBMISSION_STATUS);
      await executeSubmitTask(task);
    }
    catch(error){
      handleTaskError(error, task);
    }
  }, waitTime);
}

async function rescheduleUnproccessedTasks(firstTime){
  const release = await mutex.acquire();
  try {
    const tasks = [ ...(await getPendingTasks()) ];
    if(firstTime) {
      const failedTasks = await getFailedTasksForRetry(MAX_ATTEMPTS);
      tasks.push(...failedTasks);
    }
    for(let task of tasks){
      try {
        await updateTask(task.subject, PENDING_STATUS, task.numberOfRetries);
        await updatePublishedResourceStatus(task.involves, PENDING_SUBMISSION_STATUS);
        await executeSubmitTask(task);
      }
      catch(error){
        handleTaskError(error, task);
      }
    }
  } catch(error){
    console.error('Failed rescheduling unprocessed tasks')
  } finally {
    release();
  }
};

async function proccessResourcesWithoutTask() {
  const release = await mutex.acquire();
  const resources = await getResourcesWithoutTask();

  for(let resource of resources) {
    const resourceUri = resource.resource;
    if(!(await requiresMelding(resourceUri))){
      console.log(`No melding required for ${resourceUri}`);
      continue;
    };
    const task = await createTask(resourceUri);

    try{
      await executeSubmitTask(task);
      await updateTask(task.subject, SUCCESS_STATUS, task.numberOfRetries);
      await updatePublishedResourceStatus(task.involves, SUCCESS_SUBMISSION_STATUS);
    }
    catch(error){
      handleTaskError(error, task);
    }
  }
  release();
}

function calcTimeout(x){
  //expected to be milliseconds
  return Math.round(Math.exp(0.3 * x + 10)); //I dunno I just gave it a shot
}

new CronJob(CRON_FREQUENCY, async function() {
  try {
    await rescheduleUnproccessedTasks(false);
    await proccessResourcesWithoutTask();
  } catch (err) {
    console.log("Error with the cronJob: ");
    console.log(err);
  }
}, null, true);

new CronJob(CRON_DATA_REFRESH, async function () {
  try {
    console.log('Starting the cron job on reporting data');
    await refreshReportingData();
    console.log('Initial refresh started');
  } catch (err) {
    console.error("Error while refreshing the reporting data:", err);
  }
}, null, true);

refreshReportingData();
