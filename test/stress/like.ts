import http from 'k6/http'
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

const stepLikes = [10_000_000, 20_000_000, 50_000_000, 100_000_000]
const windowSeconds = 600
const rampSeconds = 30
const holdSeconds = 120
const p50Ms = 10
const p99Ms = 100
const abortEvalDelaySeconds = 10
const msPerSecond = 1000
const statusAccepted = 202
const userIDHeader = 'User-ID'

const baseURL = __ENV.BASE_URL
if (!baseURL) {
  throw new Error('BASE_URL is required')
}

const peakLikesPerSecond = Math.max(...stepLikes) / windowSeconds

http.setResponseCallback(http.expectedStatuses(statusAccepted))

function abortOn(threshold: string) {
  return { threshold, abortOnFail: true, delayAbortEval: `${abortEvalDelaySeconds}s` }
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    likes: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: `${windowSeconds}s`,
      stages: stepLikes.flatMap((target) => [
        { duration: `${rampSeconds}s`, target },
        { duration: `${holdSeconds}s`, target }
      ]),
      preAllocatedVUs: Math.ceil((peakLikesPerSecond * p99Ms) / msPerSecond)
    }
  },
  thresholds: {
    http_req_duration: [abortOn(`p(50)<${p50Ms}`), abortOn(`p(99)<${p99Ms}`)],
    http_req_failed: [abortOn('rate==0')],
    dropped_iterations: [abortOn('count==0')]
  }
}
export const setup= (): { postID: string } => {
  const postID = uuidv4()
  console.log(`post_id=${postID}`)
  return { postID }
}
export default (data: { postID: string }): void => {
  http.post(`${baseURL}/posts/${data.postID}/like`, null, {
    headers: { [userIDHeader]: uuidv4() }
  })
}