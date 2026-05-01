#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

readonly WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-300}"
readonly POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
readonly APP_MOUNT_PREFIX="/opt/airflow/app/"
readonly AIRFLOW_IMAGE_SERVICES=(
  streamlit
  airflow-init
  airflow-webserver
  airflow-scheduler
)
readonly BOOTSTRAP_SERVICES=(
  postgres
  opensearch
  opensearch-dashboards
  superset
  streamlit
  airflow-init
)
readonly AIRFLOW_RUNTIME_SERVICES=(
  airflow-webserver
  airflow-scheduler
)
readonly REQUIRED_MODEL_PATHS=(
  "models/slovnet_ner_news_v1.tar"
  "models/navec_news_v1_1B_250K_300d_100q.tar"
  "models/any-news-classifier"
)

log() {
  printf '[news-analyzer] %s\n' "$*"
}

warn() {
  printf '[news-analyzer] WARNING: %s\n' "$*" >&2
}

die() {
  printf '[news-analyzer] ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  local command_name="$1"
  command -v "${command_name}" >/dev/null 2>&1 || die "Required command not found: ${command_name}"
}

trim_quotes() {
  local value="$1"
  if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
    value="${value:1:${#value}-2}"
  elif [[ "${value}" == \'*\' && "${value}" == *\' ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s' "${value}"
}

read_dotenv_value() {
  local key="$1"
  local line

  line="$(grep -E "^${key}=" .env | tail -n 1 || true)"
  if [[ -z "${line}" ]]; then
    return 1
  fi

  trim_quotes "${line#*=}"
}

container_path_to_host_path() {
  local container_path="$1"

  if [[ "${container_path}" != ${APP_MOUNT_PREFIX}* ]]; then
    die "Model path ${container_path} must stay under ${APP_MOUNT_PREFIX} so the local startup script can validate it."
  fi

  printf '%s' "${container_path#${APP_MOUNT_PREFIX}}"
}

ensure_path_exists() {
  local path="$1"
  [[ -e "${path}" ]] || die "Required local path is missing: ${path}"
}

service_container_id() {
  docker compose ps -aq "$1"
}

service_state() {
  local service="$1"
  local container_id

  container_id="$(service_container_id "${service}")"
  if [[ -z "${container_id}" ]]; then
    printf 'missing'
    return 0
  fi

  docker inspect -f '{{.State.Status}}' "${container_id}"
}

wait_for_container_exit_success() {
  local service="$1"
  local timeout_seconds="$2"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    local container_id
    local state
    local exit_code

    container_id="$(service_container_id "${service}")"
    if [[ -z "${container_id}" ]]; then
      sleep "${POLL_INTERVAL_SECONDS}"
      continue
    fi

    state="$(docker inspect -f '{{.State.Status}}' "${container_id}")"
    case "${state}" in
      exited)
        exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${container_id}")"
        if [[ "${exit_code}" == "0" ]]; then
          return 0
        fi
        die "${service} finished with exit code ${exit_code}. Inspect logs: docker compose logs ${service}"
        ;;
      created|running|restarting)
        sleep "${POLL_INTERVAL_SECONDS}"
        ;;
      *)
        die "${service} entered unexpected state '${state}'. Inspect logs: docker compose logs ${service}"
        ;;
    esac
  done

  die "Timed out waiting for ${service} to complete. Inspect logs: docker compose logs ${service}"
}

wait_for_service_running() {
  local service="$1"
  local timeout_seconds="$2"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if [[ "$(service_state "${service}")" == "running" ]]; then
      return 0
    fi
    sleep "${POLL_INTERVAL_SECONDS}"
  done

  die "Timed out waiting for ${service} to reach running state. Inspect logs: docker compose logs ${service}"
}

wait_for_http_ready() {
  local name="$1"
  local url="$2"
  local timeout_seconds="$3"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if curl -fsS -o /dev/null "${url}" 2>/dev/null; then
      return 0
    fi
    sleep "${POLL_INTERVAL_SECONDS}"
  done

  return 1
}

check_prerequisites() {
  require_command docker
  require_command curl

  docker compose version >/dev/null 2>&1 || die "docker compose is not available."
  docker info >/dev/null 2>&1 || die "Docker daemon is not available. Start Docker Desktop or the Docker service first."

  [[ -f ".env" ]] || die "Missing .env in the project root."

  local airflow_secret_key
  airflow_secret_key="$(read_dotenv_value "AIRFLOW__WEBSERVER__SECRET_KEY" || true)"
  [[ -n "${airflow_secret_key}" ]] || die "AIRFLOW__WEBSERVER__SECRET_KEY must be set in .env."

  for path in "${REQUIRED_MODEL_PATHS[@]}"; do
    ensure_path_exists "${path}"
  done

  local dedup_container_path
  local dedup_host_path
  dedup_container_path="$(read_dotenv_value "DEDUP_MODEL_NAME" || true)"
  if [[ -z "${dedup_container_path}" ]]; then
    dedup_container_path="${APP_MOUNT_PREFIX}models/dedup-paraphrase-multilingual-MiniLM-L12-v2"
  fi
  dedup_host_path="$(container_path_to_host_path "${dedup_container_path}")"
  ensure_path_exists "${dedup_host_path}"

  docker compose config >/dev/null || die "docker compose configuration is invalid."
}

print_summary() {
  printf '\n'
  log "Project is ready."
  printf 'Airflow:                http://localhost:8080 (admin/admin)\n'
  printf 'Streamlit:              http://localhost:8501\n'
  printf 'OpenSearch:             http://localhost:9200\n'
  printf 'Superset:               http://localhost:8088 (admin/admin)\n'
  printf 'OpenSearch Dashboards:  http://localhost:5601\n'
  printf '\n'
  printf 'Next step: open Airflow and run DAG news_unified_pipeline.\n'
  if [[ -z "$(read_dotenv_value "GIGACHAT_AUTH_KEY" || true)" ]]; then
    printf 'Note: GIGACHAT_AUTH_KEY is empty in .env, so summary stages will be skipped.\n'
  fi
}

check_prerequisites

log "Building shared Airflow image..."
docker compose build "${AIRFLOW_IMAGE_SERVICES[@]}"

log "Starting base services..."
docker compose up -d "${BOOTSTRAP_SERVICES[@]}"

log "Waiting for airflow-init to finish..."
wait_for_container_exit_success "airflow-init" "${WAIT_TIMEOUT_SECONDS}"

log "Starting Airflow runtime services..."
docker compose up -d "${AIRFLOW_RUNTIME_SERVICES[@]}"

log "Waiting for airflow-scheduler..."
wait_for_service_running "airflow-scheduler" "${WAIT_TIMEOUT_SECONDS}"

log "Waiting for OpenSearch..."
wait_for_http_ready "OpenSearch" "http://localhost:9200" "${WAIT_TIMEOUT_SECONDS}" || die "OpenSearch is not reachable on http://localhost:9200"

log "Waiting for Streamlit..."
wait_for_http_ready "Streamlit" "http://localhost:8501" "${WAIT_TIMEOUT_SECONDS}" || die "Streamlit is not reachable on http://localhost:8501"

log "Waiting for Superset..."
wait_for_http_ready "Superset" "http://localhost:8088" "${WAIT_TIMEOUT_SECONDS}" || die "Superset is not reachable on http://localhost:8088"

log "Waiting for Airflow webserver..."
wait_for_http_ready "Airflow" "http://localhost:8080" "${WAIT_TIMEOUT_SECONDS}" || die "Airflow is not reachable on http://localhost:8080"

log "Checking OpenSearch Dashboards..."
if ! wait_for_http_ready "OpenSearch Dashboards" "http://localhost:5601" "${WAIT_TIMEOUT_SECONDS}"; then
  if [[ "$(service_state "opensearch-dashboards")" == "running" ]]; then
    warn "OpenSearch Dashboards container is running, but http://localhost:5601 did not respond before timeout."
  else
    die "OpenSearch Dashboards is not reachable and the service is not running. Inspect logs: docker compose logs opensearch-dashboards"
  fi
fi

print_summary
