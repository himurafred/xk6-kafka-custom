#!/bin/bash
# Script to build xk6-kafka Docker image with custom modifications

set -e

# Configuration
IMAGE_NAME="xk6-kafka-custom"
IMAGE_TAG="${1:-latest}"
DOCKERFILE="Dockerfile.build"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Building xk6-kafka Custom Image${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Image: ${GREEN}${IMAGE_NAME}:${IMAGE_TAG}${NC}"
echo -e "Dockerfile: ${GREEN}${DOCKERFILE}${NC}"
echo ""

# Check if Dockerfile exists
if [ ! -f "${DOCKERFILE}" ]; then
    echo -e "${RED}Error: ${DOCKERFILE} not found${NC}"
    exit 1
fi

# Build the image
echo -e "${BLUE}Building Docker image...${NC}"
docker build \
    -f "${DOCKERFILE}" \
    -t "${IMAGE_NAME}:${IMAGE_TAG}" \
    -t "${IMAGE_NAME}:latest" \
    .

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}Build completed successfully!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "Image tags created:"
    echo -e "  - ${GREEN}${IMAGE_NAME}:${IMAGE_TAG}${NC}"
    echo -e "  - ${GREEN}${IMAGE_NAME}:latest${NC}"
    echo ""
    echo -e "To use this image in k6-kafka-load-testing, update the Dockerfile:"
    echo -e "  ${BLUE}ARG XK6_KAFKA_BASE_IMAGE=${IMAGE_NAME}:${IMAGE_TAG}${NC}"
    echo ""
    echo -e "To test the image:"
    echo -e "  ${BLUE}docker run --rm ${IMAGE_NAME}:${IMAGE_TAG}${NC}"
else
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi
