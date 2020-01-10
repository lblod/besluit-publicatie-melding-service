import { app, errorHandler } from 'mu';
import { PENDING_STATUS, FAILED_STATUS, SUCCESS_STATUS } from './support/queries' ;
import { waitForDatabase } from './database-utils';
import { getPendingTasks,
         getFailedTasksForRetry,
         createTask,
         updateTask,
         getTask,
         getPublishedResourcesFromDelta,
         getPublishedResourcesWithoutAssociatedTask } from './support/queries';
import { executeSubmitTask } from './support/pipeline';
import bodyParser from 'body-parser';
import { CronJob } from 'cron';

const PENDING_TIMEOUT = process.env.PENDING_TIMEOUT_HOURS || 3;
const CRON_FREQUENCY = process.env.CACHING_CRON_PATTERN || '0 */5 * * * *';
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || 10);

waitForDatabase(rescheduleTasksOnStart);

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
  for(const pr of publishedResourceUris){
    let task = await createTask(pr);
    try{
      await executeSubmitTask(task);
    }
    catch(error){
      handleTaskError(error, task);
    }
  }
}

async function handleTaskError(error, task){
  console.error(`Error for task ${task.subject}`);
  console.error(error);
  await updateTask(task.subject, FAILED_STATUS, task.numberofRetries);

  if(parseInt(task.numberOfRetries) >= MAX_ATTEMPTS){
    await updateTask(task.uri, FAILED_STATUS, task.numberOfRetries);
    console.log(`Stopping retries for task ${task.subject})`);
  }
  else scheduleRetryProcessing(task);
}

async function scheduleRetryProcessing(task){
  console.log(`Tried processing ${task.subject} retried ${task.numberOfRetries}/${MAX_ATTEMPTS} already`);

  const waitTime = calcTimeout(parseInt(task.numberOfRetries));

  console.log(`Expecting next retry for ${task.subject} in about ${waitTime/1000} seconds`);
  setTimeout(async () => {
    try {
      console.log(`Retry for task ${task.subject}`);
      await updateTask(task.subject, PENDING_STATUS, parseInt(task.numberOfRetries) + 1);
      await executeSubmitTask(task);
    }
    catch(error){
      handleTaskError(error, task);
    }
  }, waitTime);
}

async function rescheduleTasksOnStart(){
  const tasks = [ ...(await getPendingTasks()), ...(await getFailedTasksForRetry(MAX_ATTEMPTS)) ];
  for(let task of tasks){
    try {
      await scheduleRetryProcessing(task);
    }
    catch(error){
      //if rescheduling fails, we consider there is something really broken...
      console.log(`Fatal error for ${task.subject}`);
      await updateTask(task.uri, FAILED_STATUS, task.numberOfRetries);
    }
  }
};

function calcTimeout(x){
  //expected to be milliseconds
  return Math.round(Math.exp(0.3 * x + 10)); //I dunno I just gave it a shot
}
