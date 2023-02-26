# Build and Execution Steps:

## Introdution

This project is a demonstration of how to use Pyspark as a wrapper around Scala Class/Methods. 
JVM present in sparkContext will be used to convert into JavaObjects.

## Package Structure

![image](https://user-images.githubusercontent.com/72322393/221418917-dee8bf42-0394-41e6-ba0c-0c187c0b4fd3.png)

## Installation

1. Clone the repository: `git clone https://github.com/username/project-name.git`
2. Navigate to the project directory: cd pyspark-scala-Connector/
3. Install Python dependencies: sh makePython.sh
4. Build jar: sbt package

## Usage

To run the project, do the following:
1. cd pyspark-scala-Connector/scripts/
2. sh main.sh $jar_file_path $spark_submit_path $inputPath $outputPath (4 Inputs Expected)

## Troubleshooting

If you encounter any issues while running the project, try the following solutions:

- Ensure correct scala,spark and pyspark versions are installed
- Check if java version is jdk 8.
- Best Works with version mentioned and shell scripts.
