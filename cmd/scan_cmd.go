package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

var baseDir, workDir string
var batchSize int32

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "Scan storage for path files",
	Long:  "Scan storage for path files",
	Run: func(cmd *cobra.Command, vArgs []string) {
		run()
	},
}

func init() {
	scanCmd.Flags().StringVarP(&baseDir, "base", "b", "/opt/indy/var/lib/indy/storage", "Base dir of storage for all indy artifacts.")
	scanCmd.Flags().StringVarP(&workDir, "workdir", "w", "./", "Work dir to store all generated working files.")
	scanCmd.Flags().Int32VarP(&batchSize, "batch", "B", 50000, "Batch of paths to process each time.")
}

func run() {
	printCurrentParams()
	if !validateBaseDir() {
		os.Exit(1)
	}

	prepareWorkDir()

	start := time.Now()
	var total int32 = 0
	validPkgs := []string{"generic-http", "maven", "npm"}
	validExistedPkgs := make([]string, 0)
	for _, pkg := range validPkgs {
		pkgInfo, _ := os.Stat(path.Join(baseDir, pkg))
		if pkgInfo != nil && pkgInfo.IsDir() {
			fmt.Printf("%s is a valid pkg to scan\n", pkg)
			validExistedPkgs = append(validExistedPkgs, pkg)
		}
	}
	for _, validPkg := range validExistedPkgs {
		total = total + listAndStorePkgPaths(validPkg)
	}
	end := time.Now()
	duration := end.Sub(start)
	fmt.Print("\n\n")
	fmt.Printf("File Scan completed, there are %d files need to migrate.\n", total)
	fmt.Printf("Time consumed: %f seconds\n", duration.Seconds())

	storeTotal(total)
}

func printCurrentParams() {
	fmt.Printf("Working dir for whole migration process: %s\n", workDir)
	fmt.Printf("Base storage dir for artifacts: %s\n", baseDir)
	fmt.Printf("Batch of paths to process each time: %d\n\n", batchSize)
}

func validateBaseDir() bool {

	info, err := os.Stat(baseDir)
	if os.IsNotExist(err) || !info.IsDir() {
		fmt.Printf("Error: base dir %s dose not exist or is not a directory\n", baseDir)
		return false
	}

	maven := "maven"
	mvnInfo, err := os.Stat(path.Join(baseDir, maven))
	if os.IsNotExist(err) || !mvnInfo.IsDir() {
		fmt.Printf("Error: the base dir %s is not a valid volume to store indy artifacts.\n", baseDir)
		return false
	}

	return true
}

func prepareWorkDir() {
	todoPath := path.Join(workDir, TodoFilesDir)
	createDirs(todoPath)
	processedPath := path.Join(workDir, ProcessedFilesDir)
	createDirs(processedPath)
}

func createDirs(path string) error {
	state, err := os.Stat(path)
	if os.IsExist(err) || (state != nil && state.IsDir()) {
		fmt.Printf("%s folder is not empty, will clean it first.\n", path)
		err := os.RemoveAll(path)
		if err != nil {
			fmt.Printf("Error happened during remove path %s. Error is: %s", path, err)
		}
	}
	return os.MkdirAll(path, os.ModeDir|os.ModePerm)
}

func listAndStorePkgPaths(pkg string) int32 {
	todoPrefix := TodoFilesDir + "-" + pkg
	fmt.Printf("Start to scan package %s for files\n", pkg)
	filePaths := make([]string, 0)
	var batchNum int32 = 0
	var totalNum int32 = 0
	listFunc := func(path string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			filePaths = append(filePaths, path)
			if int32(len(filePaths)) >= batchSize {
				storeBatchToFile(filePaths, todoPrefix, batchNum)
				batchNum++
				totalNum = totalNum + int32(len(filePaths))
				filePaths = filePaths[:0]
			}
		}
		return nil
	}
	pkgPath := path.Join(baseDir, pkg)
	filepath.Walk(pkgPath, listFunc)
	if len(filePaths) > 0 {
		storeBatchToFile(filePaths, todoPrefix, batchNum+1)
		totalNum = totalNum + int32(len(filePaths))
	}
	return totalNum
}

func storeBatchToFile(filePaths []string, prefix string, batch int32) {
	batchFileName := fmt.Sprintf("%s-batch-%d.txt", prefix, batch)
	batchFilePath := path.Join(workDir, TodoFilesDir, batchFileName)
	fmt.Printf("Start to store paths for batch %d to file %s\n", batch, batchFileName)
	f, err := os.OpenFile(batchFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0744)
	defer f.Close()
	if err == nil {
		datawriter := bufio.NewWriter(f)
		for _, data := range filePaths {
			_, _ = datawriter.WriteString(data + "\n")
		}

		datawriter.Flush()
	}
	fmt.Printf("Batch %d to file %s finished\n", batch, batchFileName)
}

func storeTotal(total int32) {
	statusFilePath := path.Join(baseDir, "scan_status")
	f, err := os.OpenFile(statusFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0744)
	defer f.Close()
	if err == nil {
		datawriter := bufio.NewWriter(f)
		_, _ = datawriter.WriteString(fmt.Sprintf("Total:%d\n", total))

		datawriter.Flush()
	}
	fmt.Printf("Total number stored in %s\n", statusFilePath)
}
