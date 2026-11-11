import http from 'k6/http'
import exec from 'k6/execution'
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

const totalLikes = 4_000_000
const testSeconds = 120
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

http.setResponseCallback(http.expectedStatuses(statusAccepted))

export const options = {
  discardResponseBodies: true,
  scenarios: {
    likes: {
      executor: 'constant-arrival-rate',
      rate: totalLikes,
      timeUnit: `${testSeconds}s`,
      duration: `${testSeconds}s`,
      preAllocatedVUs: Math.ceil((likesPerSecond * p99Ms) / msPerSecond)
    }
  },
  thresholds: {
    http_req_duration: [`p(50)<${p50Ms}`, `p(99)<${p99Ms}`],
    http_req_failed: ['rate==0'],
    http_reqs: [`count==${totalLikes}`],
    dropped_iterations: ['count==0']
  }
}
export const setup = (): { postID: string } => {
  const postID = uuidv4()
  console.log(`post_id=${postID} likes=${totalLikes}`)
  return { postID }
}
export default (data: { postID: string }): void => {
  if (exec.scenario.iterationInTest >= totalLikes) {
    return
  }
  http.post(`${baseURL}/posts/${data.postID}/like`, null, {
    headers: { [userIDHeader]: uuidv4() }
  })
}