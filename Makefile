.PHONY: build test format integrationTest

build:
	./gradlew clean build

test:
	./gradlew clean test

format:
	./gradlew spotlessApply

integrationTest:
	./gradlew clean integrationTest
