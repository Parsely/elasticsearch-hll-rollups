import argparse
import json
import random
import sys
import uuid
import mmh3

from itertools import cycle


def _init(num_services, jobs_per_service):
    output = []
    for _ in range(num_services):
        output.append(
            {
                "name": f"service-{str(uuid.uuid4())}",
                "jobs": [
                    mmh3.hash64(str(uuid.uuid4()))[0] for _ in range(jobs_per_service)
                ],
            }
        )
    return output


def generate_data(
    num_containers,
    num_services,
    jobs_per_service,
    jobs_per_container,
    container_job_count_variation_pct,
    running_job_pct,
):
    services = cycle(_init(num_services, jobs_per_service))
    variance = int(jobs_per_container * (container_job_count_variation_pct / 100))
    min_jobs = max(jobs_per_container - variance, 1)
    max_jobs = jobs_per_container + variance
    running_job_pct = running_job_pct / 100

    for _ in range(num_containers):
        svc = next(services)
        all_jobs = random.choices(svc["jobs"], k=random.randint(min_jobs, max_jobs))
        running_jobs = random.choices(all_jobs, k=int(len(all_jobs) * running_job_pct))
        yield {
            "service_id": svc["name"],
            "container_id": f"container-{str(uuid.uuid4())}",
            "all_jobs": all_jobs,
            "all_jobs_len": len(all_jobs),
            "running_jobs": running_jobs,
            "running_jobs_len": len(running_jobs),
        }


def main(
    num_containers,
    num_services,
    jobs_per_service,
    jobs_per_container,
    container_job_count_variation_pct,
    running_job_pct,
):
    generator = generate_data(
        num_containers,
        num_services,
        jobs_per_service,
        jobs_per_container,
        container_job_count_variation_pct,
        running_job_pct,
    )
    for _ in range(num_containers):
        print(json.dumps(next(generator)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        usage="python generate_data.py",
        description="""
    Generate data for esrally tracks.

    Generates data that could be diagnostic info from a queue/worker
    style system which is useful for testing high cardinality sets in
    ES.

    The model creates a number of services, which each have a set of
    jobs. Each ES doc generated models the state of a container
    running one of the services. It'll have a subset of jobs that
    it knows about, some of which are running.

    Jobs can be shared between containers. This means a
    cardinality agg would be needed to know how many jobs are handled
    by any given service.
    """,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "num_containers",
        metavar="NUM_CONTAINERS",
        type=int,
        help="The number of containers (ES docs) to model.",
    )
    parser.add_argument(
        "--num-services",
        metavar="NUM_SERVICES",
        type=int,
        help="Number of services to model. [default: NUM_CONTAINERS/10]",
    )
    parser.add_argument(
        "--jobs-per-service",
        type=int,
        help="The number of jobs (UUIDs) assigned to each service. [default: NUM_CONTAINERS/2]",
    )
    parser.add_argument(
        "--jobs-per-container",
        type=int,
        default=50,
        help="The number of jobs (UUIDs) assigned to each container (ES doc). Will be "
        "affected by job count variation as well. [default: 50]",
    )
    parser.add_argument(
        "--job-count-variation",
        type=int,
        default=10,
        help="How much job counts should vary between docs. Should be between 0 and 100. "
        "For example a value of 10 gives a +/- 10%% variation in the length of all_jobs "
        "between docs. [default: 10]",
    )
    parser.add_argument(
        "--running-job-percentage",
        type=int,
        default=10,
        help="Percent of `all_jobs` that should be in `running_jobs`. [default: 10]",
    )
    args = parser.parse_args()

    num_services = args.num_services or int(args.num_containers / 10)
    jobs_per_service = args.jobs_per_service or int(args.num_containers / 2)

    main(
        args.num_containers,
        num_services,
        jobs_per_service,
        args.jobs_per_container,
        args.job_count_variation,
        args.running_job_percentage,
    )
