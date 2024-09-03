package crio

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chaosblade-io/chaosblade-exec-cri/exec/container"
	"github.com/chaosblade-io/chaosblade-spec-go/spec"
	"github.com/containerd/containerd/namespaces"
	containertype "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"google.golang.org/grpc"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"time"
)

const (
	DefaultStateUinxAddress    = "unix:///var/run/crio/crio.sock"
	DefaultContainerdNameSpace = "k8s.io"
)

var cli *CRIClient

// NewClient 创建与 crio 的客户端连接
type CRIClient struct {
	runtimeService v1.RuntimeServiceClient
	conn           *grpc.ClientConn
	imageService   v1.ImageServiceClient
	Ctx            context.Context
	Cancel         context.CancelFunc
}

func NewClient(endpoint string, namespace string) (*CRIClient, error) {
	dialOptions := []grpc.DialOption{
		grpc.WithInsecure(), // 可以考虑使用安全连接
		grpc.WithBlock(),
	}

	if endpoint == "" {
		endpoint = DefaultStateUinxAddress
	}
	if namespace == "" {
		namespace = DefaultContainerdNameSpace
	}
	var (
		ctx    = context.Background()
		cancel context.CancelFunc
	)
	ctx = namespaces.WithNamespace(ctx, namespace)
	ctx, cancel = context.WithCancel(ctx)

	conn, err := grpc.DialContext(ctx, endpoint, dialOptions...)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("failed to connect to crio endpoint %s: %w", endpoint, ctx.Err())
		}
		return nil, fmt.Errorf("failed to connect to crio endpoint %s: %v", endpoint, err.Error())
	}
	runtimeService := v1.NewRuntimeServiceClient(conn)
	imageService := v1.NewImageServiceClient(conn)
	return &CRIClient{
		runtimeService: runtimeService,
		conn:           conn,
		imageService:   imageService,
		Ctx:            ctx,
		Cancel:         cancel,
	}, nil
}

// Close 关闭客户端连接
func (c *CRIClient) Close() error {
	return c.conn.Close()
}

// GetContainerById 根据容器ID获取容器的详细信息
func (c *CRIClient) GetContainerById(ctx context.Context, containerId string) (container.ContainerInfo, error, int32) {
	// 构造 GetContainerStatusRequest 请求
	var containerInfo container.ContainerInfo
	request := &v1.ContainerStatusRequest{
		ContainerId: containerId,
		Verbose:     true, // 设置为 true 获取详细信息
	}
	// 调用 RuntimeService 的 GetContainerStatus 方法
	response, err := c.runtimeService.ContainerStatus(ctx, request)
	if err != nil {
		return containerInfo, fmt.Errorf("failed to get container status for container %s: %v", containerId, err), spec.ContainerExecFailed.Code
	}
	// 检查响应
	if response == nil || response.Status == nil {
		return containerInfo, fmt.Errorf("no response status found for container %s", containerId), spec.ContainerExecFailed.Code
	}
	return convertContainerInfo(response.Status), nil, spec.OK.Code
}

func convertContainerInfo(containerDetail *v1.ContainerStatus) container.ContainerInfo {
	return container.ContainerInfo{
		ContainerId:   containerDetail.Id,
		ContainerName: containerDetail.Metadata.Name,
		//Env:             spec.Process.Env,
		Labels: containerDetail.Labels,
		Spec:   nil,
	}
}

func (c *CRIClient) GetPidById(ctx context.Context, containerId string) (int32, error, int32) {
	request := &v1.ContainerStatusRequest{
		ContainerId: containerId,
		Verbose:     true, // 设置为 true 获取详细信息
	}
	response, err := c.runtimeService.ContainerStatus(ctx, request)
	if err != nil {
		return -1, fmt.Errorf("failed to get container status and info for container %s: %v", containerId, err), spec.ContainerExecFailed.Code
	}
	if response == nil || response.Info == nil {
		return -1, fmt.Errorf("container info is nil for container %s", containerId), spec.ContainerExecFailed.Code
	}
	// 获取 Info 字段中的详细信息
	info := response.Info
	var dataMap map[string]interface{}
	// 通常 PID 存储在 Info 字段中，例如 "pid": "1234"
	//pidStr, ok := info["info"]
	//fmt.Println("pidStr:", pidStr)
	// 使用 json.Unmarshal() 解析 JSON 字符串到 map
	err = json.Unmarshal([]byte(info["info"]), &dataMap)
	if err != nil {
		return -1, fmt.Errorf("json.Unmarshal container info error for container %s,%v", containerId, err), spec.ContainerExecFailed.Code
	}
	return int32(dataMap["pid"].(float64)), nil, spec.OK.Code
}

func (c *CRIClient) GetContainerByName(ctx context.Context, containerName string) (container.ContainerInfo, error, int32) {
	// 首先列出所有容器
	var containerInfo container.ContainerInfo
	listRequest := &v1.ListContainersRequest{
		Filter: &v1.ContainerFilter{},
	}
	listResponse, err := c.runtimeService.ListContainers(ctx, listRequest)
	if err != nil {
		return containerInfo, fmt.Errorf("failed to list containers: %s", err.Error()), spec.ContainerExecFailed.Code
	}
	// 遍历容器列表，找到匹配的容器
	var containerID string
	for _, container := range listResponse.Containers {
		if container.Labels["io.kubernetes.container.name"] == containerName {
			containerID = container.Id
			break
		}
	}
	if containerID == "" {
		return containerInfo, fmt.Errorf("container with name %s not found", containerName), spec.ContainerExecFailed.Code
	}
	// 使用找到的容器ID获取容器的详细状态信息
	statusRequest := &v1.ContainerStatusRequest{
		ContainerId: containerID,
	}
	statusResponse, err := c.runtimeService.ContainerStatus(ctx, statusRequest)
	if err != nil {
		return containerInfo, fmt.Errorf("no statusResponse found for containername %s,error: %s", containerName, err.Error()), spec.ContainerExecFailed.Code
	}

	if statusResponse == nil || statusResponse.Status == nil {
		return containerInfo, fmt.Errorf("no statusResponse found for container %s", containerName), spec.ContainerExecFailed.Code
	}
	return convertContainerInfo(statusResponse.Status), nil, spec.OK.Code
}

// 标签选择器从容器运行时中筛选容器
func (c *CRIClient) GetContainerByLabelSelector(labels map[string]string) (container.ContainerInfo, error, int32) {
	var containerInfo container.ContainerInfo
	// 获取所有容器列表
	listRequest := &v1.ListContainersRequest{}
	listResponse, err := c.runtimeService.ListContainers(c.Ctx, listRequest)
	if err != nil {
		return containerInfo, fmt.Errorf("failed to list containers: %v", err), spec.ContainerExecFailed.Code
	}
	var filteredContainers []*v1.Container
	// 遍历所有容器并应用标签过滤
	for _, container := range listResponse.Containers {
		if matchLabels(container, labels) {
			filteredContainers = append(filteredContainers, container)
		}
	}
	if len(filteredContainers) == 0 {
		return containerInfo, fmt.Errorf("no containers found: %v", err), spec.ContainerExecFailed.Code
	}
	ContainerInfo := convertContainerInfo2(filteredContainers[0])
	return ContainerInfo, nil, spec.OK.Code
}

func convertContainerInfo2(containerDetail *v1.Container) container.ContainerInfo {
	return container.ContainerInfo{
		ContainerId:   containerDetail.Id,
		ContainerName: containerDetail.Metadata.Name,
		//Env:             spec.Process.Env,
		Labels: containerDetail.Labels,
		Spec:   nil,
	}
}
func matchLabels(container *v1.Container, labelSelector map[string]string) bool {
	// 获取容器的标签
	labels := container.Labels
	if labels == nil {
		return false
	}
	// 判断容器的标签是否符合选择器
	for key, value := range labelSelector {
		if containerValue, exists := labels[key]; !exists || containerValue != value {
			return false
		}
	}
	return true
}

func (c *CRIClient) RemoveContainer(ctx context.Context, containerId string, force bool) error {
	// 先尝试停止容器
	stopRequest := &v1.StopContainerRequest{
		ContainerId: containerId,
		Timeout:     15, // 你可以设置一个合理的超时时间
	}
	_, err := c.runtimeService.StopContainer(ctx, stopRequest)
	if err != nil {
		return fmt.Errorf("failed to stop container %s: %v", containerId, err)
	}
	// 然后删除容器
	removeRequest := &v1.RemoveContainerRequest{
		ContainerId: containerId,
	}
	_, err = c.runtimeService.RemoveContainer(ctx, removeRequest)
	if err != nil {
		return fmt.Errorf("failed to remove container %s: %v", containerId, err)
	}
	return nil
}

// CopyToContainer 将 tar 文件复制到容器中并解压缩
func (c *CRIClient) CopyToContainer(ctx context.Context, containerId, srcFile, dstPath, extractDirName string, override bool) error {
	processId, err, _ := c.GetPidById(ctx, containerId)
	if err != nil {
		return err
	}
	return crioCopyToContainer(ctx, uint32(processId), srcFile, dstPath, extractDirName, override)
}

func (c *CRIClient) ExecContainer(ctx context.Context, containerId, command string) (output string, err error) {
	processId, err, _ := c.GetPidById(ctx, containerId)
	if err != nil {
		return "", err
	}
	return crioExecContainer(ctx, processId, command)
}

// ExecuteAndRemove: create and start a container for executing a command, and remove the container
// ExecuteAndRemove 在容器中执行命令，然后删除容器
// todo
func (c *CRIClient) ExecuteAndRemove(ctx context.Context, config *containertype.Config, hostConfig *containertype.HostConfig,
	networkConfig *network.NetworkingConfig, containerName string, removed bool, timeout time.Duration, command string, containerInfo container.ContainerInfo) (containerId string, output string, err error, code int32) {
	// 创建容器
	containerId, err = c.CreateContainer(ctx, containerName, config, hostConfig, networkConfig)
	if err != nil {
		return "", "", fmt.Errorf("CreateContainer error:%v", err), spec.CreateContainerFailed.Code
	}
	// 启动容器
	startRequest := &v1.StartContainerRequest{
		ContainerId: containerId,
	}
	_, err = c.runtimeService.StartContainer(ctx, startRequest)
	if err != nil {
		return "", "", fmt.Errorf("StartContainer error:%v", err), spec.CreateContainerFailed.Code
	}
	var cmdslice strslice.StrSlice
	cmdslice = append(cmdslice, command)
	if config.Cmd == nil {
		config.Cmd = cmdslice
	}
	// 在容器中执行命令
	execRequest := &v1.ExecSyncRequest{
		ContainerId: containerId,
		Cmd:         config.Cmd,
		Timeout:     int64(timeout.Seconds()), // 以秒为单位
	}
	execResponse, err := c.runtimeService.ExecSync(ctx, execRequest)
	if err != nil {
		return containerId, "", fmt.Errorf("failed to execute command in container %s: %v", err), spec.CreateContainerFailed.Code
	}

	if execResponse.ExitCode != 0 {
		return containerId, "", fmt.Errorf("command in container failed : %v", err), spec.ContainerExecFailed.Code

	}
	// 停止容器
	stopRequest := &v1.StopContainerRequest{
		ContainerId: containerId,
		Timeout:     10, // 停止容器的超时时间，可以根据需要调整
	}
	_, err = c.runtimeService.StopContainer(ctx, stopRequest)
	if err != nil {
		return containerId, "", fmt.Errorf("command in container failed : %v", err), spec.ContainerExecFailed.Code
	}
	// 删除容器
	removeRequest := &v1.RemoveContainerRequest{
		ContainerId: containerId,
	}
	_, err = c.runtimeService.RemoveContainer(ctx, removeRequest)
	if err != nil {
		return containerId, "", fmt.Errorf("failed to remove container : %v", err), spec.ContainerExecFailed.Code
	}
	return containerId, execResponse.String(), nil, spec.OK.Code
}

// CreateContainer 创建一个新容器，带有配置选项
func (c *CRIClient) CreateContainer(ctx context.Context, containerName string, config *containertype.Config, hostConfig *containertype.HostConfig, networkConfig *network.NetworkingConfig) (string, error) {
	// 拉取镜像
	// check image exists or not
	imageSpec := &v1.ImageSpec{Image: config.Image}
	pullRequest := &v1.PullImageRequest{Image: imageSpec}
	statusRequest := &v1.ImageStatusRequest{Image: imageSpec}
	_, err := c.imageService.ImageStatus(ctx, statusRequest)

	_, err = c.imageService.PullImage(ctx, pullRequest)
	if err != nil {
		return "", fmt.Errorf("failed to pull image %s: %v", config.Image, err)
	}

	// 转换 container.Config 和 container.HostConfig 到 CRI 配置
	containerConfig := &v1.ContainerConfig{
		Metadata: &v1.ContainerMetadata{
			Name: containerName,
		},
		Image:   imageSpec,
		Command: config.Cmd,
		Args:    config.Entrypoint,
		// 将 config、hostConfig 和 networkConfig 映射到 CRI 的相应字段
		//Envs:        []string{config.Env},
		Labels:     config.Labels,
		WorkingDir: config.WorkingDir,
	}

	// 设置Linux容器配置
	linuxConfig := &v1.LinuxContainerConfig{
		Resources: &v1.LinuxContainerResources{
			MemoryLimitInBytes: hostConfig.Memory, // 从 HostConfig 获取内存限制
		},
	}

	containerConfig.Linux = linuxConfig

	// 创建容器
	containerRequest := &v1.CreateContainerRequest{
		//PodSandboxId:  podSandboxId,
		Config:        containerConfig,
		SandboxConfig: &v1.PodSandboxConfig{}, // 如果有网络配置，可以将 networkConfig 映射到 CRI 的 PodSandboxConfig
	}

	containerResponse, err := c.runtimeService.CreateContainer(ctx, containerRequest)
	if err != nil {
		return "", fmt.Errorf("failed to create container: %v", err)
	}

	return containerResponse.ContainerId, nil
}
