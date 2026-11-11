import http from 'k6/http'
import exec from 'k6/execution'
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

const totalLikes = 4_000_000
const peakViewers = 16_000
const minWatchSeconds = 10
const maxWatchSeconds = 60
const testSeconds = 120
const viewerRampSeconds = 30
const p50Ms = 50
const p99Ms = 200
const msPerSecond = 1000
const statusAccepted = 202
const userIDHeader = 'User-ID'

const baseURL = __ENV.BASE_URL
if (!baseURL) {
  throw new Error('BASE_URL is required')
}

const likesPerSecond = totalLikes / testSeconds
const avgWatchSeconds = (minWatchSeconds + maxWatchSeconds) / 2
const viewerArrivalsPerSecond = Math.ceil(peakViewers / avgWatchSeconds)

http.setResponseCallback(http.expectedStatuses(statusAccepted))

export const options = {
  discardResponseBodies: true,
  scenarios: {
    likes: {
      executor: 'constant-arrival-rate',
      rate: totalLikes,
      timeUnit: `${testSeconds}s`,
      duration: `${testSeconds}s`,
      preAllocatedVUs: Math.ceil((likesPerSecond * p99Ms) / msPerSecond),
      exec: 'like'
    },
    viewers: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      stages: [
        { duration: `${viewerRampSeconds}s`, target: viewerArrivalsPerSecond },
        { duration: `${testSeconds - 2 * viewerRampSeconds}s`, target: viewerArrivalsPerSecond },
        { duration: `${viewerRampSeconds}s`, target: 0 }
      ],
      preAllocatedVUs: viewerArrivalsPerSecond,
      maxVUs: peakViewers,
      exec: 'watch'
    }
  },
  thresholds: {
    'http_req_duration{scenario:likes}': [`p(50)<${p50Ms}`, `p(99)<${p99Ms}`],
    'http_req_failed{scenario:likes}': ['rate==0'],
    'http_reqs{scenario:likes}': [`count==${totalLikes}`],
    'dropped_iterations{scenario:likes}': ['count==0']
  }
}
export const setup = (): { postID: string } => {
  const postID = uuidv4()
  console.log(`post_id=${postID} likes=${totalLikes}`)
  return { postID }
}
export const like = (data: { postID: string }): void => {
  if (exec.scenario.iterationInTest >= totalLikes) {
    return
  }
  http.post(`${baseURL}/posts/${data.postID}/like`, null, {
    headers: { [userIDHeader]: uuidv4() }
  })
}
export const watch = (data: { postID: string }): void => {
  const watchSeconds = minWatchSeconds + Math.random() * (maxWatchSeconds - minWatchSeconds)
  http.get(`${baseURL}/posts/${data.postID}/likes`, { timeout: `${watchSeconds}s` })
}