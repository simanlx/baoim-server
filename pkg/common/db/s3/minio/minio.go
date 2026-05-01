// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package minio 实现S3接口规范的MinIO对象存储客户端，用于文件上传、下载、分片、签名等
package minio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"BaoIM-Server/pkg/common/db/cache"
	"BaoIM-Server/pkg/common/db/s3"
	"baoim/tools/errs"
	"baoim/tools/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/signer"
)

// unsignedPayload S3签名中标识不对请求体进行签名
const (
	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// 分片上传常量定义
// minPartSize: 单个分片最小大小 5MB
// maxPartSize: 单个分片最大允许大小 5GB
// maxNumSize:  单个文件最多允许多少个分片
const (
	minPartSize int64 = 1024 * 1024 * 5        // 5MB
	maxPartSize int64 = 1024 * 1024 * 1024 * 5 // 5GB
	maxNumSize  int64 = 10000
)

// 图片相关限制常量
// maxImageWidth: 缩略图最大宽度
// maxImageHeight: 缩略图最大高度
// maxImageSize: 原图最大限制大小
// imageThumbnailPath: 缩略图存储路径
const (
	maxImageWidth      = 1024
	maxImageHeight     = 1024
	maxImageSize       = 1024 * 1024 * 50
	imageThumbnailPath = "openim/thumbnail"
)

// successCode 表单上传成功后返回的HTTP状态码
const successCode = http.StatusOK

// Config MinIO连接配置结构体，所有连接MinIO需要的参数
type Config struct {
	Bucket          string // 对象存储桶名称
	Endpoint        string // MinIO服务地址，包含协议
	AccessKeyID     string // 访问密钥ID
	SecretAccessKey string // 访问密钥密码
	SessionToken    string // 临时会话token，非临时密钥可空
	SignEndpoint    string // 单独用于签名的服务地址，可与Endpoint不同
	PublicRead      bool   // 是否开启桶公共读权限
}

// NewMinio 初始化MinIO客户端实例，实现s3.Interface接口
// 参数：cache 缓存实例，conf MinIO配置
// 返回：s3接口实例、错误信息
func NewMinio(cache cache.MinioCache, conf Config) (s3.Interface, error) {
	// 解析Endpoint地址，获取协议、主机、路径等信息
	u, err := url.Parse(conf.Endpoint)
	if err != nil {
		return nil, err
	}

	// 构建MinIO客户端配置项，设置认证信息与是否启用HTTPS
	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(conf.AccessKeyID, conf.SecretAccessKey, conf.SessionToken),
		Secure: u.Scheme == "https",
	}

	// 创建MinIO基础客户端
	client, err := minio.New(u.Host, opts)
	if err != nil {
		return nil, err
	}

	// 初始化MinIO结构体实例
	m := &Minio{
		conf:   conf,
		bucket: conf.Bucket,
		core:   &minio.Core{Client: client}, // 底层Core客户端，提供更底层API
		lock:   &sync.Mutex{},               // 初始化互斥锁，防止并发重复初始化
		init:   false,                       // 初始化状态标记
		cache:  cache,                       // 缓存实例
	}

	// 判断是否使用独立签名Endpoint，分别配置签名客户端
	if conf.SignEndpoint == "" || conf.SignEndpoint == conf.Endpoint {
		// 签名地址与主服务一致，直接使用同一套配置
		m.opts = opts
		m.sign = m.core.Client
		m.prefix = u.Path
		u.Path = ""
		conf.Endpoint = u.String()
		m.signEndpoint = conf.Endpoint
	} else {
		// 解析独立签名地址
		su, err := url.Parse(conf.SignEndpoint)
		if err != nil {
			return nil, err
		}

		// 创建签名专用客户端配置
		m.opts = &minio.Options{
			Creds:  credentials.NewStaticV4(conf.AccessKeyID, conf.SecretAccessKey, conf.SessionToken),
			Secure: su.Scheme == "https",
		}

		// 创建签名客户端
		m.sign, err = minio.New(su.Host, m.opts)
		if err != nil {
			return nil, err
		}

		m.prefix = su.Path
		su.Path = ""
		conf.SignEndpoint = su.String()
		m.signEndpoint = conf.SignEndpoint
	}

	// 创建超时上下文，执行MinIO初始化（检查桶、创建桶、配置权限）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.initMinio(ctx); err != nil {
		fmt.Println("init minio error:", err)
	}

	return m, nil
}

// Minio 实现s3.Interface接口的核心结构体
type Minio struct {
	conf         Config           // 配置信息
	bucket       string           // 存储桶名称
	signEndpoint string           // 签名服务地址
	location     string           // 桶区域
	opts         *minio.Options   // 客户端选项
	core         *minio.Core      // 底层Core API客户端
	sign         *minio.Client    // 签名专用客户端
	lock         sync.Locker      // 初始化锁
	init         bool             // 是否已初始化完成
	prefix       string           // URL路径前缀
	cache        cache.MinioCache // 缓存
}

// initMinio 初始化MinIO环境：检查桶是否存在，不存在则创建；配置公共读策略；获取区域信息
// 使用双重检查锁定，确保全局只执行一次初始化
func (m *Minio) initMinio(ctx context.Context) error {
	// 第一次检查：已初始化直接返回
	if m.init {
		return nil
	}

	// 加锁，防止并发初始化
	m.lock.Lock()
	defer m.lock.Unlock()

	// 第二次检查：锁竞争后再次确认是否已初始化
	if m.init {
		return nil
	}

	// 检查存储桶是否已存在
	exists, err := m.core.Client.BucketExists(ctx, m.conf.Bucket)
	if err != nil {
		return fmt.Errorf("check bucket exists error: %w", err)
	}

	// 桶不存在则创建
	if !exists {
		if err = m.core.Client.MakeBucket(ctx, m.conf.Bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("make bucket error: %w", err)
		}
	}

	// 如果开启公共读，设置桶策略，允许匿名获取/上传对象
	if m.conf.PublicRead {
		policy := fmt.Sprintf(
			`{"Version": "2012-10-17","Statement": [{"Action": ["s3:GetObject","s3:PutObject"],"Effect": "Allow","Principal": {"AWS": ["*"]},"Resource": ["arn:aws:s3:::%s/*"],"Sid": ""}]}`,
			m.conf.Bucket,
		)
		if err = m.core.Client.SetBucketPolicy(ctx, m.conf.Bucket, policy); err != nil {
			return err
		}
	}

	// 获取桶所在区域信息
	m.location, err = m.core.Client.GetBucketLocation(ctx, m.conf.Bucket)
	if err != nil {
		return err
	}

	// 匿名函数：通过反射强行设置签名客户端的桶区域缓存（兼容minio-go私有字段）
	func() {
		// 非独立签名地址直接跳过
		if m.conf.SignEndpoint == "" || m.conf.SignEndpoint == m.conf.Endpoint {
			return
		}

		// 捕获panic，避免反射操作崩溃
		defer func() {
			if r := recover(); r != nil {
				m.sign = m.core.Client
				log.ZWarn(
					context.Background(),
					"set sign bucket location cache panic",
					errors.New("failed to get private field value"),
					"recover",
					fmt.Sprintf("%+v", r),
					"development version",
					"github.com/minio/minio-go/v7 v7.0.61",
				)
			}
		}()

		// 反射获取bucketLocCache私有字段
		blc := reflect.ValueOf(m.sign).Elem().FieldByName("bucketLocCache")
		vblc := reflect.New(reflect.PtrTo(blc.Type()))
		*(*unsafe.Pointer)(vblc.UnsafePointer()) = unsafe.Pointer(blc.UnsafeAddr())
		// 调用Set方法缓存桶区域
		vblc.Elem().Elem().Interface().(interface{ Set(string, string) }).Set(m.conf.Bucket, m.location)
	}()

	// 标记初始化完成
	m.init = true
	return nil
}

// Engine 返回当前存储引擎标识：minio
func (m *Minio) Engine() string {
	return "minio"
}

// PartLimit 返回分片上传限制：最小/最大分片大小、最大分片数量
func (m *Minio) PartLimit() *s3.PartLimit {
	return &s3.PartLimit{
		MinPartSize: minPartSize,
		MaxPartSize: maxPartSize,
		MaxNumSize:  maxNumSize,
	}
}

// InitiateMultipartUpload 初始化分片上传，获取UploadID
// 参数：name 对象路径
// 返回：包含UploadID的结果、错误
func (m *Minio) InitiateMultipartUpload(ctx context.Context, name string) (*s3.InitiateMultipartUploadResult, error) {
	// 确保已完成初始化
	if err := m.initMinio(ctx); err != nil {
		return nil, err
	}

	// 创建分片上传任务，获取UploadID
	uploadID, err := m.core.NewMultipartUpload(ctx, m.bucket, name, minio.PutObjectOptions{})
	if err != nil {
		return nil, err
	}

	// 组装返回结果
	return &s3.InitiateMultipartUploadResult{
		Bucket:   m.bucket,
		Key:      name,
		UploadID: uploadID,
	}, nil
}

// CompleteMultipartUpload 完成分片上传，合并所有已上传分片
// 参数：uploadID 分片任务ID，name 对象名，parts 已上传分片列表
func (m *Minio) CompleteMultipartUpload(ctx context.Context, uploadID string, name string, parts []s3.Part) (*s3.CompleteMultipartUploadResult, error) {
	if err := m.initMinio(ctx); err != nil {
		return nil, err
	}

	// 转换分片格式
	minioParts := make([]minio.CompletePart, len(parts))
	for i, part := range parts {
		minioParts[i] = minio.CompletePart{
			PartNumber: part.PartNumber,
			ETag:       strings.ToLower(part.ETag),
		}
	}

	// 调用Core API完成分片合并
	upload, err := m.core.CompleteMultipartUpload(ctx, m.bucket, name, uploadID, minioParts, minio.PutObjectOptions{})
	if err != nil {
		return nil, err
	}

	// 删除缓存中该对象的图片信息
	m.delObjectImageInfoKey(ctx, name, upload.Size)

	// 组装返回结果
	return &s3.CompleteMultipartUploadResult{
		Location: upload.Location,
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		ETag:     strings.ToLower(upload.ETag),
	}, nil
}

// PartSize 根据文件总大小自动计算推荐分片大小
func (m *Minio) PartSize(ctx context.Context, size int64) (int64, error) {
	// 文件大小必须大于0
	if size <= 0 {
		return 0, errors.New("size must be greater than 0")
	}

	// 超过最大允许文件大小（5GB*10000）报错
	if size > maxPartSize*maxNumSize {
		return 0, fmt.Errorf("MINIO size must be less than the maximum allowed limit")
	}

	// 小文件使用固定最小分片5MB
	if size <= minPartSize*maxNumSize {
		return minPartSize, nil
	}

	// 大文件自动均分分片，保证分片数量不超过maxNumSize
	partSize := size / maxNumSize
	if size%maxNumSize != 0 {
		partSize++
	}

	return partSize, nil
}

// AuthSign 对分片上传请求进行签名，返回每个分片的签名信息
func (m *Minio) AuthSign(ctx context.Context, uploadID string, name string, expire time.Duration, partNumbers []int) (*s3.AuthSignResult, error) {
	if err := m.initMinio(ctx); err != nil {
		return nil, err
	}

	// 获取认证密钥信息
	creds, err := m.opts.Creds.Get()
	if err != nil {
		return nil, err
	}

	// 初始化返回结果
	result := s3.AuthSignResult{
		URL:   m.signEndpoint + "/" + m.bucket + "/" + name,
		Query: url.Values{"uploadId": {uploadID}},
		Parts: make([]s3.SignPart, len(partNumbers)),
	}

	// 遍历所有分片编号，逐个生成签名
	for i, partNumber := range partNumbers {
		// 构建分片上传URL
		rawURL := result.URL + "?partNumber=" + strconv.Itoa(partNumber) + "&uploadId=" + uploadID
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, rawURL, nil)
		if err != nil {
			return nil, err
		}

		// 设置不签名请求体
		request.Header.Set("X-Amz-Content-Sha256", unsignedPayload)

		// 使用S3 V4签名算法签名请求
		request = signer.SignV4Trailer(*request, creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken, m.location, nil)

		// 存入当前分片的签名信息
		result.Parts[i] = s3.SignPart{
			PartNumber: partNumber,
			Query:      url.Values{"partNumber": {strconv.Itoa(partNumber)}},
			Header:     request.Header,
		}
	}

	// 如果配置了URL前缀，拼接前缀
	if m.prefix != "" {
		result.URL = m.signEndpoint + m.prefix + "/" + m.bucket + "/" + name
	}

	return &result, nil
}

// PresignedPutObject 生成文件直传的预签名URL
func (m *Minio) PresignedPutObject(ctx context.Context, name string, expire time.Duration) (string, error) {
	if err := m.initMinio(ctx); err != nil {
		return "", err
	}

	// 调用MinIO SDK生成上传签名
	rawURL, err := m.sign.PresignedPutObject(ctx, m.bucket, name, expire)
	if err != nil {
		return "", err
	}

	// 拼接路径前缀
	if m.prefix != "" {
		rawURL.Path = path.Join(m.prefix, rawURL.Path)
	}

	return rawURL.String(), nil
}

// DeleteObject 删除指定对象
func (m *Minio) DeleteObject(ctx context.Context, name string) error {
	if err := m.initMinio(ctx); err != nil {
		return err
	}

	return m.core.Client.RemoveObject(ctx, m.bucket, name, minio.RemoveObjectOptions{})
}

// StatObject 获取对象元信息（大小、ETag、修改时间等）
func (m *Minio) StatObject(ctx context.Context, name string) (*s3.ObjectInfo, error) {
	if err := m.initMinio(ctx); err != nil {
		return nil, err
	}

	info, err := m.core.Client.StatObject(ctx, m.bucket, name, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	// 转换为通用s3.ObjectInfo结构
	return &s3.ObjectInfo{
		ETag:         strings.ToLower(info.ETag),
		Key:          info.Key,
		Size:         info.Size,
		LastModified: info.LastModified,
	}, nil
}

// CopyObject 同桶内复制对象
func (m *Minio) CopyObject(ctx context.Context, src string, dst string) (*s3.CopyObjectInfo, error) {
	if err := m.initMinio(ctx); err != nil {
		return nil, err
	}

	// 执行复制操作
	result, err := m.core.Client.CopyObject(ctx, minio.CopyDestOptions{
		Bucket: m.bucket,
		Object: dst,
	}, minio.CopySrcOptions{
		Bucket: m.bucket,
		Object: src,
	})
	if err != nil {
		return nil, err
	}

	return &s3.CopyObjectInfo{
		Key:  dst,
		ETag: strings.ToLower(result.ETag),
	}, nil
}

// IsNotFound 判断错误是否为“对象不存在”
func (m *Minio) IsNotFound(err error) bool {
	switch e := errs.Unwrap(err).(type) {
	case minio.ErrorResponse:
		return e.StatusCode == http.StatusNotFound || e.Code == "NoSuchKey"
	case *minio.ErrorResponse:
		return e.StatusCode == http.StatusNotFound || e.Code == "NoSuchKey"
	default:
		return false
	}
}

// AbortMultipartUpload 取消分片上传任务
func (m *Minio) AbortMultipartUpload(ctx context.Context, uploadID string, name string) error {
	if err := m.initMinio(ctx); err != nil {
		return err
	}

	return m.core.AbortMultipartUpload(ctx, m.bucket, name, uploadID)
}

// ListUploadedParts 查询已上传的分片列表
func (m *Minio) ListUploadedParts(ctx context.Context, uploadID string, name string, partNumberMarker int, maxParts int) (*s3.ListUploadedPartsResult, error) {
	if err := m.initMinio(ctx); err != nil {
		return nil, err
	}

	// 调用SDK查询分片
	result, err := m.core.ListObjectParts(ctx, m.bucket, name, uploadID, partNumberMarker, maxParts)
	if err != nil {
		return nil, err
	}

	// 转换为通用结构
	res := &s3.ListUploadedPartsResult{
		Key:                  result.Key,
		UploadID:             result.UploadID,
		MaxParts:             result.MaxParts,
		NextPartNumberMarker: result.NextPartNumberMarker,
		UploadedParts:        make([]s3.UploadedPart, len(result.ObjectParts)),
	}

	for i, part := range result.ObjectParts {
		res.UploadedParts[i] = s3.UploadedPart{
			PartNumber:   part.PartNumber,
			LastModified: part.LastModified,
			ETag:         part.ETag,
			Size:         part.Size,
		}
	}

	return res, nil
}

// PresignedGetObject 生成下载预签名URL；公共读桶直接生成公开URL
func (m *Minio) PresignedGetObject(ctx context.Context, name string, expire time.Duration, query url.Values) (string, error) {
	// 设置默认过期时间：99年；最小过期时间1秒
	if expire <= 0 {
		expire = time.Hour * 24 * 365 * 99
	} else if expire < time.Second {
		expire = time.Second
	}

	var (
		rawURL *url.URL
		err    error
	)

	// 公共读：直接构造URL；私有：生成签名URL
	if m.conf.PublicRead {
		rawURL, err = makeTargetURL(m.sign, m.bucket, name, m.location, false, query)
	} else {
		rawURL, err = m.sign.PresignedGetObject(ctx, m.bucket, name, expire, query)
	}
	if err != nil {
		return "", err
	}

	// 拼接路径前缀
	if m.prefix != "" {
		rawURL.Path = path.Join(m.prefix, rawURL.Path)
	}

	return rawURL.String(), nil
}

// AccessURL 获取对象访问URL，支持图片缩略图、下载文件名、ContentType自定义
func (m *Minio) AccessURL(ctx context.Context, name string, expire time.Duration, opt *s3.AccessURLOption) (string, error) {
	if err := m.initMinio(ctx); err != nil {
		return "", err
	}

	// 构造响应头参数：ContentType、下载文件名
	reqParams := make(url.Values)
	if opt != nil {
		if opt.ContentType != "" {
			reqParams.Set("response-content-type", opt.ContentType)
		}
		if opt.Filename != "" {
			reqParams.Set("response-content-disposition", `attachment; filename=`+strconv.Quote(opt.Filename))
		}
	}

	// 不满足缩略图条件，返回普通访问URL
	if opt.Image == nil || (opt.Image.Width < 0 && opt.Image.Height < 0 && opt.Image.Format == "") || (opt.Image.Width > maxImageWidth || opt.Image.Height > maxImageHeight) {
		return m.PresignedGetObject(ctx, name, expire, reqParams)
	}

	// 满足条件，生成缩略图URL
	return m.getImageThumbnailURL(ctx, name, expire, opt.Image)
}

// getObjectData 读取对象内容，支持限制读取长度
func (m *Minio) getObjectData(ctx context.Context, name string, limit int64) ([]byte, error) {
	// 获取对象流
	object, err := m.core.Client.GetObject(ctx, m.bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()

	// limit<0 读取全部；否则读取指定字节
	if limit < 0 {
		return io.ReadAll(object)
	}
	return io.ReadAll(io.LimitReader(object, limit))
}

// FormData 生成浏览器表单直传所需的签名表单数据
func (m *Minio) FormData(ctx context.Context, name string, size int64, contentType string, duration time.Duration) (*s3.FormData, error) {
	if err := m.initMinio(ctx); err != nil {
		return nil, err
	}

	// 创建表单上传策略
	policy := minio.NewPostPolicy()

	// 设置上传文件key
	if err := policy.SetKey(name); err != nil {
		return nil, err
	}

	// 设置策略过期时间
	expires := time.Now().Add(duration)
	if err := policy.SetExpires(expires); err != nil {
		return nil, err
	}

	// 设置文件大小限制
	if size > 0 {
		if err := policy.SetContentLengthRange(0, size); err != nil {
			return nil, err
		}
	}

	// 设置成功响应状态码
	if err := policy.SetSuccessStatusAction(strconv.Itoa(successCode)); err != nil {
		return nil, err
	}

	// 设置文件类型
	if contentType != "" {
		if err := policy.SetContentType(contentType); err != nil {
			return nil, err
		}
	}

	// 设置存储桶
	if err := policy.SetBucket(m.bucket); err != nil {
		return nil, err
	}

	// 生成表单签名信息
	u, fd, err := m.core.PresignedPostPolicy(ctx, policy)
	if err != nil {
		return nil, err
	}

	// 将上传地址替换为签名服务地址
	sign, err := url.Parse(m.signEndpoint)
	if err != nil {
		return nil, err
	}
	u.Scheme = sign.Scheme
	u.Host = sign.Host

	// 返回表单上传所需全部信息
	return &s3.FormData{
		URL:          u.String(),
		File:         "file",
		Header:       nil,
		FormData:     fd,
		Expires:      expires,
		SuccessCodes: []int{successCode},
	}, nil
}
