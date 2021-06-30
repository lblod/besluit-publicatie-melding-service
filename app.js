import { app, errorHandler } from 'mu';
import {
  PENDING_STATUS,
  FAILED_STATUS,
  SUCCESS_STATUS,
  PENDING_SUBMISSION_STATUS,
  FAILED_SUBMISSION_STATUS,
  SUCCESS_SUBMISSION_STATUS
} from './support/queries' ;
import { waitForDatabase } from './database-utils';
import { 
  getPendingTasks,
  requiresMelding,
  getTaskForResource,
  getFailedTasksForRetry,
  createTask,
  updateTask,
  updatePublishedResourceStatus,
  getPublishedResourcesFromDelta 
} from './support/queries';
import { executeSubmitTask } from './support/pipeline';
import bodyParser from 'body-parser';
import { CronJob } from 'cron';
import AsyncLock from 'async-lock'

const lock = new AsyncLock();

const CRON_FREQUENCY = process.env.RESCHEDULE_CRON_PATTERN || '0 0 * * *';
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

app.use(errorHandler);

async function processPublishedResources(publishedResourceUris){
  lock.acquire('taskProcessing', async () => {
    for(const pr of publishedResourceUris){

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
        await executeSubmitTask(task);
        await updateTask(task.subject, SUCCESS_STATUS, task.numberOfRetries);
        await updatePublishedResourceStatus(task.involves, SUCCESS_SUBMISSION_STATUS);
      }
      catch(error){
        handleTaskError(error, task);
      }
    }
  }
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
  lock.acquire('taskProcessing', async () => {
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
        //if rescheduling fails, we consider there is something really broken...
        console.log(`Fatal error for ${task.subject}`);
        await updateTask(task.subject, FAILED_STATUS, task.numberOfRetries);
        await updatePublishedResourceStatus(task.involves, FAILED_SUBMISSION_STATUS);
      }
    }
  })
};

function calcTimeout(x){
  //expected to be milliseconds
  return Math.round(Math.exp(0.3 * x + 10)); //I dunno I just gave it a shot
}

new CronJob(CRON_FREQUENCY, async function() {
  try {
    await rescheduleUnproccessedTasks(false)
  } catch (err) {
    console.log("Error with the cronJob: ");
    console.log(err);
  }
}, null, true);
