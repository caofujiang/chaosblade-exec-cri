package crio

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/chaosblade-io/chaosblade-spec-go/log"
	"github.com/chaosblade-io/chaosblade-spec-go/spec"
	"github.com/chaosblade-io/chaosblade-spec-go/util"
	"os"
	"os/exec"
	"path"
	"strings"
)

func crioCopyToContainer(ctx context.Context, pid uint32, srcFile, dstPath, extractDirName string, override bool) error {

	args := fmt.Sprintf("-t %d -p -m -- /bin/sh -c", pid)
	argsArray := strings.Split(args, " ")
	nsbin := path.Join(util.GetProgramPath(), "bin", spec.NSExecBin)

	command := fmt.Sprintf("cat > %s", path.Join(dstPath, path.Base(srcFile)))
	log.Infof(ctx, "run copy cmd: %s %s %s", nsbin, args, command)

	cmd := exec.Command(nsbin, append(argsArray, command)...)

	var outMsg bytes.Buffer
	var errMsg bytes.Buffer
	cmd.Stdout = &outMsg
	cmd.Stderr = &errMsg

	open, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer open.Close()
	cmd.Stdin = open
	err = cmd.Run()
	log.Debugf(ctx, "Command Result, output: %s, errMsg: %s,  err: %v", outMsg.String(), errMsg.String(), err)
	if err != nil {
		return err
	}

	if errMsg.Len() != 0 {
		return errors.New(errMsg.String())
	}

	// tar -zxf
	command = fmt.Sprintf("-t %d -p -m -- tar -zxf %s -C %s", pid, path.Join(dstPath, path.Base(srcFile)), dstPath)
	log.Infof(ctx, "run tar cmd: %s %s", nsbin, command)
	cmd = exec.Command(nsbin, strings.Split(command, " ")...)
	//
	var outMsg2 bytes.Buffer
	var errMsg2 bytes.Buffer
	cmd.Stdout = &outMsg2
	cmd.Stderr = &errMsg2
	err = cmd.Run()
	log.Debugf(ctx, "Tar Command Result, output: %s, errMsg: %s,  err: %v", outMsg2.String(), errMsg2.String(), err)
	if err != nil {
		return err
	}

	if errMsg2.Len() != 0 {
		return errors.New(errMsg.String())
	}

	return nil
}

func crioExecContainer(ctx context.Context, pid int32, command string) (output string, err error) {

	args := fmt.Sprintf("-t %d -p -m -n -- /bin/sh -c", pid)
	argsArray := strings.Split(args, " ")
	nsbin := path.Join(util.GetProgramPath(), "bin", spec.NSExecBin)

	log.Infof(ctx, "exec container cmd: %s %s %s", nsbin, args, command)

	cmd := exec.Command(nsbin, append(argsArray, command)...)

	var outMsg bytes.Buffer
	var errMsg bytes.Buffer
	cmd.Stdout = &outMsg
	cmd.Stderr = &errMsg
	err = cmd.Run()

	log.Debugf(ctx, "Command Result, output: %s, errMsg: %s, err: %v", outMsg.String(), errMsg.String(), err)

	if err != nil {
		return "", err
	}
	if errMsg.Len() > 0 {
		return errMsg.String(), nil
	}

	return outMsg.String(), nil
}
