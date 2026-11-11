import http from 'k6/http'
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

const stepLikes = [10_000_000, 20_000_000, 50_000_000, 100_000_000]
const windowSeconds = 600
const rampSeconds = 30
const holdSeconds = 120
const viewersPerLikePerSecond = 0.48
const minWatchSeconds = 10
const maxWatchSeconds = 60
const p50Ms = 50
const p99Ms = 200
const abortEvalDelaySeconds = 10
const msPerSecond = 1000
const statusAccepted = 202
const userIDHeader = 'User-ID'

const baseURL = __ENV.BASE_URL
if (!baseURL) {
  throw new Error('BASE_URL is required')
}

const firstLikesPerSecond = stepLikes[0] / windowSeconds
const peakLikesPerSecond = Math.max(...stepLikes) / windowSeconds
const avgWatchSeconds = (minWatchSeconds + maxWatchSeconds) / 2

http.setResponseCallback(http.expectedStatuses(statusAccepted))

function abortOn(threshold: string) {
  return { threshold, abortOnFail: true, delayAbortEval: `${abortEvalDelaySeconds}s` }
}

function viewerArrivalsPerSecond(likes: number): number {
  return Math.ceil(((likes / windowSeconds) * viewersPerLikePerSecond) / avgWatchSeconds)
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
      preAllocatedVUs: Math.ceil((firstLikesPerSecond * p99Ms) / msPerSecond),
      maxVUs: Math.ceil((peakLikesPerSecond * p99Ms) / msPerSecond),
      exec: 'like'
    },
    viewers: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      stages: stepLikes.flatMap((likes) => [
        { duration: `${rampSeconds}s`, target: viewerArrivalsPerSecond(likes) },
        { duration: `${holdSeconds}s`, target: viewerArrivalsPerSecond(likes) }
      ]),
      preAllocatedVUs: Math.ceil(firstLikesPerSecond * viewersPerLikePerSecond),
      maxVUs: Math.ceil(peakLikesPerSecond * viewersPerLikePerSecond),
      exec: 'watch'
    }
  },
  thresholds: {
    'http_req_duration{scenario:likes}': [abortOn(`p(50)<${p50Ms}`), abortOn(`p(99)<${p99Ms}`)],
    'http_req_failed{scenario:likes}': [abortOn('rate==0')],
    'dropped_iterations{scenario:likes}': [abortOn('count==0')]
  }
}
export const setup= (): { postID: string } => {
  const postID = uuidv4()
  console.log(`post_id=${postID}`)
  return { postID }
}
export const like = (data: { postID: string }): void => {
  http.post(`${baseURL}/posts/${data.postID}/like`, null, {
    headers: { [userIDHeader]: uuidv4() }
  })
}
export const watch = (data: { postID: string }): void => {
  const watchSeconds = minWatchSeconds + Math.random() * (maxWatchSeconds - minWatchSeconds)
  http.get(`${baseURL}/posts/${data.postID}/likes`, { timeout: `${watchSeconds}s` })
}