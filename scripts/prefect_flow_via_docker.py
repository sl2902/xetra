import os
from dotenv import load_dotenv
from gcs_to_bq import main
from prefect.deployments import Deployment
from prefect.infrastructure import DockerContainer
from prefect.blocks.system import JSON

prefect_docker_block = "de-prefect-docker"

def create_docker_image() -> None:
    docker = DockerContainer(
        image=PREFECT_DOCKER_IMAGE,
        image_pull_policy="ALWAYS",
        auto_remove=True,
    )
    docker.save(prefect_docker_block, overwrite=True)

def deploy_docker_container(docker_container_block) -> None:
    deployment = Deployment.build_from_flow(
        flow=main,
        name="de-prefect-docker",
        infrastructure=docker_container_block,
        path="scripts/",
        # parameters={"prefix": "2022-04-15", "history_file": "null"},
        # entrypoint="scripts/gcs_to_bq.py:main",
        ignore_file=".prefectignore",
        skip_upload=True,
    )
    deployment.apply()


if __name__ == "__main__":
    # load_dotenv()
    env = JSON.load("de-prefect-config")
    PREFECT_DOCKER_IMAGE = env.value["PREFECT_DOCKER_IMAGE"]
    create_docker_image()
    # deploy job on Docker image
    docker_container_block = DockerContainer.load(prefect_docker_block)
    deploy_docker_container(docker_container_block)