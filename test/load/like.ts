import http from 'k6/http'
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js'

const totalLikes = 20_000_000
const testSeconds = 600
const p50Ms = 10
const p99Ms = 100
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
    dropped_iterations: ['count==0']
  }
}
export const setup = (): { postID: string } => {
  const postID = uuidv4()
  console.log(`post_id=${postID}`)
  return { postID }
}
export default (data: { postID: string }): void => {
  http.post(`${baseURL}/posts/${data.postID}/like`, null, {
    headers: { [userIDHeader]: uuidv4() }
  })
}