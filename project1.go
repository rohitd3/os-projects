// Project 1
// File System
// Rohit De
// rde1
// 30271270

package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// globals

var disk [64][512]int
var descriptors [192][4]int
var oftBuffer [4][512]int
var oftCurrentPosition [4]int
var oftFileSize [4]int
var oftDescriptorIndex [4]int
var oftValid [4]bool
var oftLoadedBlock [4]int
var memory [512]int
var output []string

// HELPER FUNCTIONS

func convertBytesToInteger(b0 int, b1 int, b2 int, b3 int) int {
	return b0*16777216 + b1*65536 + b2*256 + b3
}

func convertIntegerToBytes(val int, buffer []int, pos int) {
	buffer[pos] = (val / 16777216) % 256
	buffer[pos+1] = (val / 65536) % 256
	buffer[pos+2] = (val / 256) % 256
	buffer[pos+3] = val % 256
}

func getFileNameAtPosition(pos int) string {
	name := ""
	for i := 0; i < 4; i++ {
		c := oftBuffer[0][pos+i]
		if c != 0 {
			name = name + string(rune(c))
		}
	}
	return name
}

func searchDirectoryForFile(filename string) int {
	dirSize := oftFileSize[0]
	for pos := 0; pos < dirSize; pos += 8 {
		name := getFileNameAtPosition(pos)
		if name == filename {
			index := convertBytesToInteger(oftBuffer[0][pos+4], oftBuffer[0][pos+5], oftBuffer[0][pos+6], oftBuffer[0][pos+7])
			return index
		}
	}
	return -1
}

func insertDirectoryEntry(filename string, descriptorIndx int) bool {
	if searchDirectoryForFile(filename) != -1 {
		return false
	}

	dirSize := oftFileSize[0]
	if dirSize+8 > 512 {
		return false
	}

	pos := dirSize
	for i := 0; i < 4; i++ {
		if i < len(filename) {
			oftBuffer[0][pos+i] = int(filename[i])
		} else {
			oftBuffer[0][pos+i] = 0
		}
	}

	convertIntegerToBytes(descriptorIndx, oftBuffer[0][:], pos+4)
	oftFileSize[0] += 8
	return true
}

func deleteDirectoryEntry(filename string) int {
	dirSize := oftFileSize[0]
	for pos := 0; pos < dirSize; pos += 8 {
		name := getFileNameAtPosition(pos)
		if name == filename {
			index := convertBytesToInteger(oftBuffer[0][pos+4], oftBuffer[0][pos+5], oftBuffer[0][pos+6], oftBuffer[0][pos+7])
			for i := 0; i < 8; i++ {
				oftBuffer[0][pos+i] = 0
			}
			return index
		}
	}
	return -1
}

func buildDirectoryListing() string {
	dirSize := oftFileSize[0]
	result := ""
	first := true
	for pos := 0; pos < dirSize; pos += 8 {
		name := getFileNameAtPosition(pos)
		if name == "" {
			continue
		}
		index := convertBytesToInteger(oftBuffer[0][pos+4], oftBuffer[0][pos+5], oftBuffer[0][pos+6], oftBuffer[0][pos+7])
		desc := readDescriptor(index)
		length := desc[0]
		if !first {
			result = result + " "
		}
		result = result + name + " " + strconv.Itoa(length)
		first = false
	}
	return result
}

func descriptorInDirectory(descriptorIndx int) bool {
	dirSize := oftFileSize[0]
	for pos := 0; pos < dirSize; pos += 8 {
		indexVal := convertBytesToInteger(oftBuffer[0][pos+4], oftBuffer[0][pos+5], oftBuffer[0][pos+6], oftBuffer[0][pos+7])
		if indexVal == descriptorIndx {
			return true
		}
	}
	return false
}

func findAvailableOFTSlot() int {
	for i := 1; i < 4; i++ {
		if !oftValid[i] {
			return i
		}
	}
	return -1
}

func findFreeBlock() int {
	usedBlocks := make(map[int]bool)
	for i := 0; i < 8; i++ {
		usedBlocks[i] = true
	}

	for i := 0; i < 192; i++ {
		for j := 1; j < 4; j++ {
			blockID := descriptors[i][j]
			if blockID != 0 {
				usedBlocks[blockID] = true
			}
		}
	}

	for blockNum := 8; blockNum < 64; blockNum++ {
		if !usedBlocks[blockNum] {
			return blockNum
		}
	}
	return -1
}

func releaseBlock(blockNum int) {
}

func loadFileBlockIntoBuffer(oftIndex int, blockIndex int) {
	descIndex := oftDescriptorIndex[oftIndex]
	desc := readDescriptor(descIndex)
	fileSize := oftFileSize[oftIndex]
	oldBlockIndex := oftLoadedBlock[oftIndex]
	oldBlockNum := desc[1+oldBlockIndex]

	if fileSize > 0 {
		if oldBlockNum == 0 {
			newb := findFreeBlock()
			if newb < 0 {
				return
			}
			desc[1+oldBlockIndex] = newb
			oldBlockNum = newb
		}
		writeBlock(oldBlockNum, oftBuffer[oftIndex][:])
	}

	writeDescriptor(descIndex, desc)
	oftLoadedBlock[oftIndex] = blockIndex
	newBlockNum := desc[1+blockIndex]
	if newBlockNum != 0 {
		readBlock(newBlockNum, oftBuffer[oftIndex][:])
	} else {
		for i := 0; i < 512; i++ {
			oftBuffer[oftIndex][i] = 0
		}
	}
}

func saveDirectoryToDisk() {
	d0 := readDescriptor(0)
	block1 := d0[1]
	writeBlock(block1, oftBuffer[0][:])
	d0[0] = oftFileSize[0]
	writeDescriptor(0, d0)
}

func initializeDirectoryOFT() {
	oftValid[0] = true
	oftDescriptorIndex[0] = 0
	oftLoadedBlock[0] = 0
	d0 := readDescriptor(0)
	oftFileSize[0] = d0[0]
	oftCurrentPosition[0] = 0
	block1 := d0[1]
	if block1 != 0 {
		readBlock(block1, oftBuffer[0][:])
	} else {
		for i := 0; i < 512; i++ {
			oftBuffer[0][i] = 0
		}
	}
}

func finalizeDirectoryOFT() {
	if oftValid[0] {
		d0 := readDescriptor(0)
		block1 := d0[1]
		writeBlock(block1, oftBuffer[0][:])
		d0[0] = oftFileSize[0]
		writeDescriptor(0, d0)
		oftValid[0] = false
	}
}

// DISK ACCESS FUNCTIONS

func read_block(blockNum int, buffer []int) {
	if blockNum >= 0 && blockNum < 64 {
		for i := 0; i < 512; i++ {
			buffer[i] = disk[blockNum][i]
		}
	}
}

func write_block(blockNum int, buffer []int) {
	if blockNum >= 0 && blockNum < 64 {
		for i := 0; i < 512; i++ {
			disk[blockNum][i] = buffer[i]
		}
	}
}

func readBlock(blockNum int, buffer []int) {
	read_block(blockNum, buffer)
}

func writeBlock(blockNum int, buffer []int) {
	write_block(blockNum, buffer)
}

func readDescriptor(i int) [4]int {
	var desc [4]int
	for j := 0; j < 4; j++ {
		desc[j] = descriptors[i][j]
	}
	return desc
}

func writeDescriptor(i int, desc [4]int) {
	for j := 0; j < 4; j++ {
		descriptors[i][j] = desc[j]
	}
}

// MAIN FILE SYSTEM FUNCTIONS

// init_fs initializes
func init_fs() {
	// clear disk
	for i := 0; i < 64; i++ {
		for j := 0; j < 512; j++ {
			disk[i][j] = 0
		}
	}

	// clear descriptors
	for i := 0; i < 192; i++ {
		for j := 0; j < 4; j++ {
			descriptors[i][j] = 0
		}
	}
	descriptors[0][0] = 0
	descriptors[0][1] = 7
	descriptors[0][2] = 0
	descriptors[0][3] = 0

	for i := 0; i < 4; i++ {
		for j := 0; j < 512; j++ {
			oftBuffer[i][j] = 0
		}
		oftCurrentPosition[i] = 0
		oftFileSize[i] = 0
		oftDescriptorIndex[i] = -1
		oftValid[i] = false
		oftLoadedBlock[i] = 0
	}

	// clear memory
	for i := 0; i < 512; i++ {
		memory[i] = 0
	}
	initializeDirectoryOFT()
	output = append(output, "system initialized")
}

// creates a new file with the given name
func create(name string) {
	if len(name) > 3 {
		output = append(output, "error")
		return
	}

	if searchDirectoryForFile(name) != -1 {
		output = append(output, "error")
		return
	}

	// find free descriptor
	descriptorIndx := -1
	for i := 1; i < 192; i++ {
		d := readDescriptor(i)
		if d[0] == 0 && d[1] == 0 && d[2] == 0 && d[3] == 0 {
			if !descriptorInDirectory(i) {
				descriptorIndx = i
				break
			}
		}
	}

	if descriptorIndx == -1 {
		output = append(output, "error")
		return
	}

	var emptyDesc [4]int
	emptyDesc[0] = 0
	emptyDesc[1] = 0
	emptyDesc[2] = 0
	emptyDesc[3] = 0
	writeDescriptor(descriptorIndx, emptyDesc)

	if !insertDirectoryEntry(name, descriptorIndx) {
		output = append(output, "error")
		return
	}
	saveDirectoryToDisk()
	output = append(output, name+" created")
}

func destroy(name string) {
	descriptorIndxCheck := searchDirectoryForFile(name)
	if descriptorIndxCheck != -1 {
		for i := 1; i < 4; i++ {
			if oftValid[i] && oftDescriptorIndex[i] == descriptorIndxCheck {
				output = append(output, "error")
				return
			}
		}
	}

	descriptorIndx := deleteDirectoryEntry(name)
	if descriptorIndx == -1 {
		output = append(output, "error")
		return
	}

	desc := readDescriptor(descriptorIndx)
	for j := 1; j < 4; j++ {
		if desc[j] != 0 {
			releaseBlock(desc[j])
		}
	}

	var emptyDesc [4]int
	emptyDesc[0] = 0
	emptyDesc[1] = 0
	emptyDesc[2] = 0
	emptyDesc[3] = 0
	writeDescriptor(descriptorIndx, emptyDesc)
	saveDirectoryToDisk()
	output = append(output, name+" destroyed")
}

// opens a file by name
func open(name string) {
	descriptorIndx := searchDirectoryForFile(name)
	if descriptorIndx == -1 {
		output = append(output, "error")
		return
	}

	// check if open
	for i := 0; i < 4; i++ {
		if oftValid[i] && oftDescriptorIndex[i] == descriptorIndx {
			output = append(output, "error")
			return
		}
	}

	slot := findAvailableOFTSlot()
	if slot == -1 {
		output = append(output, "error")
		return
	}

	desc := readDescriptor(descriptorIndx)
	oftValid[slot] = true
	oftDescriptorIndex[slot] = descriptorIndx
	oftFileSize[slot] = desc[0]
	oftCurrentPosition[slot] = 0
	oftLoadedBlock[slot] = 0
	block0 := desc[1]
	if block0 != 0 {
		readBlock(block0, oftBuffer[slot][:])
	} else {
		for i := 0; i < 512; i++ {
			oftBuffer[slot][i] = 0
		}
	}

	output = append(output, name+" opened "+strconv.Itoa(slot))
}

func close_file(index int) {
	if index < 0 || index >= 4 || !oftValid[index] {
		output = append(output, "error")
		return
	}

	descIndex := oftDescriptorIndex[index]
	desc := readDescriptor(descIndex)
	fileSize := oftFileSize[index]
	if fileSize > 0 {
		blockNum := desc[1+oftLoadedBlock[index]]
		if blockNum == 0 {
			blockNum = findFreeBlock()
			if blockNum < 0 {
				output = append(output, "error")
				return
			}
			desc[1+oftLoadedBlock[index]] = blockNum
		}
		writeBlock(blockNum, oftBuffer[index][:])
	}

	desc[0] = fileSize
	writeDescriptor(descIndex, desc)
	oftValid[index] = false
	oftDescriptorIndex[index] = -1
	oftFileSize[index] = 0
	oftCurrentPosition[index] = 0
	oftLoadedBlock[index] = 0
	for i := 0; i < 512; i++ {
		oftBuffer[index][i] = 0
	}

	output = append(output, strconv.Itoa(index)+" closed")
}

func read(oftIndex int, memoryOffset int, count int) {
	if oftIndex < 0 || oftIndex >= 4 || !oftValid[oftIndex] {
		output = append(output, "error")
		return
	}
	if memoryOffset < 0 || memoryOffset+count > 512 || count < 0 {
		output = append(output, "error")
		return
	}
	fileSize := oftFileSize[oftIndex]
	curPos := oftCurrentPosition[oftIndex]
	if curPos >= fileSize {
		output = append(output, "0 bytes read from "+strconv.Itoa(oftIndex))
		return
	}
	totalRead := 0
	remaining := count

	for remaining > 0 && curPos < fileSize && curPos < 3*512 {
		blockIndex := curPos / 512
		offsetInBlock := curPos % 512

		if blockIndex != oftLoadedBlock[oftIndex] {
			loadFileBlockIntoBuffer(oftIndex, blockIndex)
		}
		spaceInBlock := 512 - offsetInBlock
		canRead := remaining
		if canRead > spaceInBlock {
			canRead = spaceInBlock
		}
		if canRead > fileSize-curPos {
			canRead = fileSize - curPos
		}

		for i := 0; i < canRead; i++ {
			memory[memoryOffset+totalRead+i] = oftBuffer[oftIndex][offsetInBlock+i]
		}
		curPos += canRead
		totalRead += canRead
		remaining -= canRead
	}

	oftCurrentPosition[oftIndex] = curPos
	output = append(output, strconv.Itoa(totalRead)+" bytes read from "+strconv.Itoa(oftIndex))
}

func write(oftIndex int, memoryOffset int, count int) {
	if oftIndex < 0 || oftIndex >= 4 || !oftValid[oftIndex] {
		output = append(output, "error")
		return
	}
	if memoryOffset < 0 || memoryOffset+count > 512 || count < 0 {
		output = append(output, "error")
		return
	}

	fileSize := oftFileSize[oftIndex]
	curPos := oftCurrentPosition[oftIndex]
	descIndex := oftDescriptorIndex[oftIndex]

	totalWritten := 0
	remaining := count

	for remaining > 0 && curPos < 3*512 {
		blockIndex := curPos / 512
		offsetInBlock := curPos % 512

		if blockIndex != oftLoadedBlock[oftIndex] {
			loadFileBlockIntoBuffer(oftIndex, blockIndex)
		}

		spaceInBlock := 512 - offsetInBlock
		toWrite := remaining
		if toWrite > spaceInBlock {
			toWrite = spaceInBlock
		}

		for i := 0; i < toWrite; i++ {
			oftBuffer[oftIndex][offsetInBlock+i] = memory[memoryOffset+totalWritten+i]
		}
		curPos += toWrite
		remaining -= toWrite
		totalWritten += toWrite

		d := readDescriptor(descIndex)
		realBlock := d[1+blockIndex]

		if realBlock == 0 {
			realBlock = findFreeBlock()
			if realBlock < 0 {
				break
			}
			d[1+blockIndex] = realBlock
		}
		writeBlock(realBlock, oftBuffer[oftIndex][:])
		writeDescriptor(descIndex, d)

		if curPos > fileSize {
			fileSize = curPos
		}
	}

	oftFileSize[oftIndex] = fileSize
	oftCurrentPosition[oftIndex] = curPos

	desc := readDescriptor(descIndex)

	desc[0] = fileSize

	writeDescriptor(descIndex, desc)

	output = append(output, strconv.Itoa(totalWritten)+" bytes written to "+strconv.Itoa(oftIndex))
}

func seek(index int, pos int) {
	if index < 0 || index >= 4 || !oftValid[index] {
		output = append(output, "error")
		return
	}
	if pos < 0 {
		output = append(output, "error")
		return
	}

	fileSize := oftFileSize[index]
	if pos > fileSize {
		output = append(output, "error")
		return
	}
	if pos > 3*512 {
		output = append(output, "error")
		return
	}

	oldPos := oftCurrentPosition[index]
	oldBlockIndex := oldPos / 512
	newBlockIndex := pos / 512

	if newBlockIndex != oldBlockIndex {
		loadFileBlockIntoBuffer(index, newBlockIndex)
	}

	oftCurrentPosition[index] = pos
	output = append(output, "position is "+strconv.Itoa(pos))
}

func directory() {
	listing := buildDirectoryListing()
	output = append(output, listing)
}

// MEMORY FUNCTIONS

func write_memory(memoryOffset int, dataString string) {
	if memoryOffset < 0 || memoryOffset >= 512 {
		output = append(output, "error")
		return
	}
	n := len(dataString)
	if memoryOffset+n > 512 {
		n = 512 - memoryOffset
	}
	for i := 0; i < n; i++ {
		memory[memoryOffset+i] = int(dataString[i])
	}
	output = append(output, strconv.Itoa(n)+" bytes written to M")
}

func read_memory(memoryOffset int, count int) {
	if memoryOffset < 0 || memoryOffset+count > 512 || count < 0 {
		output = append(output, "error")
		return
	}
	data := ""
	for i := 0; i < count; i++ {
		c := memory[memoryOffset+i]
		if c != 0 {
			data = data + string(rune(c))
		}
	}
	output = append(output, data)
}

// MAIN FUNCTION

func main() {
	inputFile, err := os.Open("input.txt")
	if err != nil {
		fmt.Println("Error opening input.txt:", err)
		return
	}
	defer inputFile.Close()

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		command_parts := strings.Fields(line)
		input_command := command_parts[0]

		if input_command == "in" {
			finalizeDirectoryOFT()
			if len(output) > 0 {
				output = append(output, "")
			}
			init_fs()
		} else if input_command == "cr" {
			if len(command_parts) < 2 {
				output = append(output, "error")
			} else {
				create(command_parts[1])
			}
		} else if input_command == "de" {
			if len(command_parts) < 2 {
				output = append(output, "error")
			} else {
				destroy(command_parts[1])
			}
		} else if input_command == "dr" {
			directory()
		} else if input_command == "op" {
			if len(command_parts) < 2 {
				output = append(output, "error")
			} else {
				open(command_parts[1])
			}
		} else if input_command == "cl" {
			if len(command_parts) < 2 {
				output = append(output, "error")
			} else {
				index, err := strconv.Atoi(command_parts[1])
				if err != nil {
					output = append(output, "error")
				} else {
					close_file(index)
				}
			}
		} else if input_command == "sk" {
			if len(command_parts) < 3 {
				output = append(output, "error")
			} else {
				index, err1 := strconv.Atoi(command_parts[1])
				pos, err2 := strconv.Atoi(command_parts[2])
				if err1 != nil || err2 != nil {
					output = append(output, "error")
				} else {
					seek(index, pos)
				}
			}
		} else if input_command == "wm" {
			if len(command_parts) < 3 {
				output = append(output, "error")
			} else {
				memoryOffset, err := strconv.Atoi(command_parts[1])
				if err != nil {
					output = append(output, "error")
				} else {
					dataString := strings.Join(command_parts[2:], " ")
					write_memory(memoryOffset, dataString)
				}
			}
		} else if input_command == "rd" {
			if len(command_parts) < 4 {
				output = append(output, "error")
			} else {
				index, err1 := strconv.Atoi(command_parts[1])
				memOff, err2 := strconv.Atoi(command_parts[2])
				cnt, err3 := strconv.Atoi(command_parts[3])
				if err1 != nil || err2 != nil || err3 != nil {
					output = append(output, "error")
				} else {
					read(index, memOff, cnt)
				}
			}
		} else if input_command == "wr" {
			if len(command_parts) < 4 {
				output = append(output, "error")
			} else {
				index, err1 := strconv.Atoi(command_parts[1])
				memOff, err2 := strconv.Atoi(command_parts[2])
				cnt, err3 := strconv.Atoi(command_parts[3])
				if err1 != nil || err2 != nil || err3 != nil {
					output = append(output, "error")
				} else {
					write(index, memOff, cnt)
				}
			}
		} else if input_command == "rm" {
			if len(command_parts) < 3 {
				output = append(output, "error")
			} else {
				memOff, err1 := strconv.Atoi(command_parts[1])
				cnt, err2 := strconv.Atoi(command_parts[2])
				if err1 != nil || err2 != nil {
					output = append(output, "error")
				} else {
					read_memory(memOff, cnt)
				}
			}
		} else {
			output = append(output, "error")
		}
	}

	outputFile, err := os.Create("output.txt")
	if err != nil {
		fmt.Println("Error creating output.txt:", err)
		return
	}

	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	for i := 0; i < len(output); i++ {
		writer.WriteString(output[i] + "\n")
	}
	writer.Flush()
}
