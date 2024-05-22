from prefect.infrastructure.docker import DockerContainer
import argparse


def save_image(image_name, name, registery=None) -> None:
    docker_container = DockerContainer( image=image_name, auto_remove=True, image_registry=registery)
    docker_container.save(name=name,)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Save a Docker container")
    parser.add_argument("--image", required=True, help="Docker image to run")
    parser.add_argument("--name", required=True, help="Name of the block")
    parser.add_argument("--registery", required=False, help="Docker registery (the block which it contains), default docker.io")
    args = parser.parse_args()
    save_image(args.image, args.name, args.registery)
