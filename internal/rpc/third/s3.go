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

package third

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path"
	"strconv"
	"time"

	"BaoIM-Server/pkg/common/db/s3"
	"BaoIM-Server/pkg/common/db/s3/cont"
	"BaoIM-Server/pkg/common/db/table/relation"
	"baoim/protocol/third"
	"baoim/tools/errs"
	"baoim/tools/log"
	"baoim/tools/mcontext"
	"baoim/tools/utils"
	"github.com/google/uuid"
)

func (t *thirdServer) PartLimit(ctx context.Context, req *third.PartLimitReq) (*third.PartLimitResp, error) {
	limit := t.s3dataBase.PartLimit()
	return &third.PartLimitResp{
		MinPartSize: limit.MinPartSize,
		MaxPartSize: limit.MaxPartSize,
		MaxNumSize:  int32(limit.MaxNumSize),
	}, nil
}

func (t *thirdServer) PartSize(ctx context.Context, req *third.PartSizeReq) (*third.PartSizeResp, error) {
	size, err := t.s3dataBase.PartSize(ctx, req.Size)
	if err != nil {
		return nil, err
	}
	return &third.PartSizeResp{Size: size}, nil
}

func (t *thirdServer) InitiateMultipartUpload(ctx context.Context, req *third.InitiateMultipartUploadReq) (*third.InitiateMultipartUploadResp, error) {
	defer log.ZDebug(ctx, "return")
	if err := t.checkUploadName(ctx, req.Name); err != nil {
		return nil, err
	}
	expireTime := time.Now().Add(t.defaultExpire)
	result, err := t.s3dataBase.InitiateMultipartUpload(ctx, req.Hash, req.Size, t.defaultExpire, int(req.MaxParts))
	if err != nil {
		if haErr, ok := errs.Unwrap(err).(*cont.HashAlreadyExistsError); ok {
			obj := &relation.ObjectModel{
				Name:        req.Name,
				UserID:      mcontext.GetOpUserID(ctx),
				Hash:        req.Hash,
				Key:         haErr.Object.Key,
				Size:        haErr.Object.Size,
				ContentType: req.ContentType,
				Group:       req.Cause,
				CreateTime:  time.Now(),
			}
			if err := t.s3dataBase.SetObject(ctx, obj); err != nil {
				return nil, err
			}
			return &third.InitiateMultipartUploadResp{
				Url: t.apiAddress(obj.Name),
			}, nil
		}
		return nil, err
	}
	var sign *third.AuthSignParts
	if result.Sign != nil && len(result.Sign.Parts) > 0 {
		sign = &third.AuthSignParts{
			Url:    result.Sign.URL,
			Query:  toPbMapArray(result.Sign.Query),
			Header: toPbMapArray(result.Sign.Header),
			Parts:  make([]*third.SignPart, len(result.Sign.Parts)),
		}
		for i, part := range result.Sign.Parts {
			sign.Parts[i] = &third.SignPart{
				PartNumber: int32(part.PartNumber),
				Url:        part.URL,
				Query:      toPbMapArray(part.Query),
				Header:     toPbMapArray(part.Header),
			}
		}
	}
	return &third.InitiateMultipartUploadResp{
		Upload: &third.UploadInfo{
			UploadID:   result.UploadID,
			PartSize:   result.PartSize,
			Sign:       sign,
			ExpireTime: expireTime.UnixMilli(),
		},
	}, nil
}

// AuthSign 第三方服务鉴权签名方法，处理上传文件的分片签名请求
// ctx: 上下文，用于传递请求链路信息、控制超时等
// req: 鉴权签名请求参数，包含上传ID、待签名的分片编号列表
// 返回值: 鉴权签名响应结果（包含签名URL、请求参数、请求头、各分片签名信息）/ 执行错误
func (t *thirdServer) AuthSign(ctx context.Context, req *third.AuthSignReq) (*third.AuthSignResp, error) {
	// 延迟打印调试日志，方法返回时自动执行，用于追踪方法执行结束
	defer log.ZDebug(ctx, "return")

	// 类型转换：将请求中的int32类型分片编号切片，转换为int类型切片，适配底层方法参数类型
	partNumbers := utils.Slice(req.PartNumbers, func(partNumber int32) int { return int(partNumber) })

	// 调用底层s3数据库服务，执行鉴权签名逻辑，获取签名结果
	result, err := t.s3dataBase.AuthSign(ctx, req.UploadID, partNumbers)
	// 签名执行失败，直接返回错误
	if err != nil {
		return nil, err
	}

	// 初始化proto协议的响应对象，组装基础签名信息
	// toPbMapArray：将原生map转换为proto协议支持的map数组格式
	resp := &third.AuthSignResp{
		Url:    result.URL,                                 // 主上传签名URL
		Query:  toPbMapArray(result.Query),                 // 签名请求参数
		Header: toPbMapArray(result.Header),                // 签名请求头
		Parts:  make([]*third.SignPart, len(result.Parts)), // 初始化分片签名列表，长度与结果一致
	}

	// 遍历底层返回的分片签名结果，逐个转换为proto协议的分片签名对象
	for i, part := range result.Parts {
		resp.Parts[i] = &third.SignPart{
			PartNumber: int32(part.PartNumber),    // 分片编号，转回int32类型适配proto
			Url:        part.URL,                  // 当前分片的签名URL
			Query:      toPbMapArray(part.Query),  // 当前分片的签名请求参数
			Header:     toPbMapArray(part.Header), // 当前分片的签名请求头
		}
	}

	// 签名处理完成，返回组装好的响应对象
	return resp, nil
}

// CompleteMultipartUpload 完成分片上传
// 核心逻辑：校验文件名 -> 调用底层S3完成分片上传 -> 保存文件元数据 -> 返回文件访问地址
// ctx: 上下文，用于传递请求信息、日志、用户信息等
// req: 完成分片上传的请求参数，包含文件名、上传ID、分片信息、文件类型等
// 返回值: 上传完成响应（含文件访问地址）、错误信息
func (t *thirdServer) CompleteMultipartUpload(ctx context.Context, req *third.CompleteMultipartUploadReq) (*third.CompleteMultipartUploadResp, error) {
	// 函数退出时打印调试日志，用于追踪函数执行结束
	defer log.ZDebug(ctx, "return")

	// 校验上传的文件名是否合法（格式、长度、非法字符等校验）
	if err := t.checkUploadName(ctx, req.Name); err != nil {
		return nil, err
	}

	// 调用底层S3存储服务，完成分片上传合并
	// 参数：上传ID(唯一标识本次分片上传)、分片列表(所有已上传的分片信息)
	result, err := t.s3dataBase.CompleteMultipartUpload(ctx, req.UploadID, req.Parts)
	if err != nil {
		return nil, err
	}

	// 构建文件对象元数据模型，用于持久化存储
	obj := &relation.ObjectModel{
		Name:        req.Name,                  // 文件名
		UserID:      mcontext.GetOpUserID(ctx), // 操作人用户ID，从上下文获取
		Hash:        result.Hash,               // 文件哈希值（用于校验文件完整性）
		Key:         result.Key,                // 存储服务中的文件唯一key
		Size:        result.Size,               // 文件总大小
		ContentType: req.ContentType,           // 文件类型（如image/jpeg、application/pdf）
		Group:       req.Cause,                 // 文件分组/业务标识
		CreateTime:  time.Now(),                // 文件创建时间（当前时间）
	}

	// 将文件元数据保存到数据库/存储中
	if err := t.s3dataBase.SetObject(ctx, obj); err != nil {
		return nil, err
	}

	// 构造响应结果，返回文件的访问地址
	return &third.CompleteMultipartUploadResp{
		Url: t.apiAddress(obj.Name), // 生成文件对外访问的URL
	}, nil
}

func (t *thirdServer) AccessURL(ctx context.Context, req *third.AccessURLReq) (*third.AccessURLResp, error) {
	opt := &s3.AccessURLOption{}
	if len(req.Query) > 0 {
		switch req.Query["type"] {
		case "":
		case "image":
			opt.Image = &s3.Image{}
			opt.Image.Format = req.Query["format"]
			opt.Image.Width, _ = strconv.Atoi(req.Query["width"])
			opt.Image.Height, _ = strconv.Atoi(req.Query["height"])
			log.ZDebug(ctx, "AccessURL image", "name", req.Name, "option", opt.Image)
		default:
			return nil, errs.ErrArgs.Wrap("invalid query type")
		}
	}
	expireTime, rawURL, err := t.s3dataBase.AccessURL(ctx, req.Name, t.defaultExpire, opt)
	if err != nil {
		return nil, err
	}
	return &third.AccessURLResp{
		Url:        rawURL,
		ExpireTime: expireTime.UnixMilli(),
	}, nil
}

func (t *thirdServer) InitiateFormData(ctx context.Context, req *third.InitiateFormDataReq) (*third.InitiateFormDataResp, error) {
	if req.Name == "" {
		return nil, errs.ErrArgs.Wrap("name is empty")
	}
	if req.Size <= 0 {
		return nil, errs.ErrArgs.Wrap("size must be greater than 0")
	}
	if err := t.checkUploadName(ctx, req.Name); err != nil {
		return nil, err
	}
	var duration time.Duration
	opUserID := mcontext.GetOpUserID(ctx)
	var key string
	if t.IsManagerUserID(opUserID) {
		if req.Millisecond <= 0 {
			duration = time.Minute * 10
		} else {
			duration = time.Millisecond * time.Duration(req.Millisecond)
		}
		if req.Absolute {
			key = req.Name
		}
	} else {
		duration = time.Minute * 10
	}
	uid, err := uuid.NewRandom()
	if err != nil {
		return nil, errs.Wrap(err, "uuid NewRandom failed")
	}
	if key == "" {
		date := time.Now().Format("20060102")

		//增加上传目录 用flutter 原生上传  不使用sdk
		path1 := cont.DirectPath
		if req.Group == "avatar" {
			path1 = cont.AvatarPath
			date = "" //取消时间目录
		} else if req.Group == "working" {
			path1 = cont.WorkingPath
			date = ""
		} else if req.Group == "user" {
			path1 = cont.UserPath
			date = ""
		} else if req.Group == "face" {
			path1 = cont.FacePath
			date = ""
		}
		key = path.Join(path1, date, opUserID, hex.EncodeToString(uid[:])+path.Ext(req.Name))
	}

	mate := FormDataMate{
		Name:        req.Name,
		Size:        req.Size,
		ContentType: req.ContentType,
		Group:       req.Group,
		Key:         key,
	}
	mateData, err := json.Marshal(&mate)
	if err != nil {
		return nil, errs.Wrap(err, "marshal failed")
	}
	resp, err := t.s3dataBase.FormData(ctx, key, req.Size, req.ContentType, duration)
	if err != nil {
		return nil, err
	}
	return &third.InitiateFormDataResp{
		Id:       base64.RawStdEncoding.EncodeToString(mateData),
		Url:      resp.URL,
		File:     resp.File,
		Header:   toPbMapArray(resp.Header),
		FormData: resp.FormData,
		Expires:  resp.Expires.UnixMilli(),
		SuccessCodes: utils.Slice(resp.SuccessCodes, func(code int) int32 {
			return int32(code)
		}),
	}, nil
}

// CompleteFormData 完成表单数据处理（文件上传确认）
// 功能：解析Base64编码的ID、校验文件信息、保存文件元数据到数据库、返回文件访问地址
func (t *thirdServer) CompleteFormData(ctx context.Context, req *third.CompleteFormDataReq) (*third.CompleteFormDataResp, error) {
	// 1. 参数校验：请求ID不能为空
	if req.Id == "" {
		return nil, errs.ErrArgs.Wrap("id is empty")
	}
	// 2. Base64解码：将请求中的ID字符串进行Raw标准Base64解码
	data, err := base64.RawStdEncoding.DecodeString(req.Id)
	if err != nil {
		return nil, errs.ErrArgs.Wrap("invalid id " + err.Error())
	}
	// 3. JSON反序列化：将解码后的数据解析为文件元数据结构体
	var mate FormDataMate
	if err := json.Unmarshal(data, &mate); err != nil {
		return nil, errs.ErrArgs.Wrap("invalid id " + err.Error())
	}
	// 4. 校验上传文件名合法性
	if err := t.checkUploadName(ctx, mate.Name); err != nil {
		return nil, err
	}
	// 5. 查询S3存储中的文件信息（校验文件是否存在）
	info, err := t.s3dataBase.StatObject(ctx, mate.Key)
	if err != nil {
		return nil, err
	}
	// 6. 校验文件大小：文件实际大小与元数据大小必须一致
	if info.Size > 0 && info.Size != mate.Size {
		return nil, errs.ErrData.Wrap("file size mismatch")
	}
	// 7. 构建文件对象模型，准备存入数据库
	obj := &relation.ObjectModel{
		Name:        mate.Name,                 // 文件名
		UserID:      mcontext.GetOpUserID(ctx), // 操作人用户ID
		Hash:        "etag_" + info.ETag,       // 文件哈希值（基于S3的ETag生成）
		Key:         info.Key,                  // S3存储唯一标识
		Size:        info.Size,                 // 文件大小
		ContentType: mate.ContentType,          // 文件类型
		Group:       mate.Group,                // 文件分组
		CreateTime:  time.Now(),                // 创建时间
	}

	// 8. 将文件元数据保存到数据库
	if err := t.s3dataBase.SetObject(ctx, obj); err != nil {
		return nil, err
	}

	// 9. 生成文件访问地址并返回
	return &third.CompleteFormDataResp{Url: t.apiAddress(mate.Name)}, nil
}

func (t *thirdServer) apiAddress(name string) string {
	return t.apiURL + name
}

type FormDataMate struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
	Group       string `json:"group"`
	Key         string `json:"key"`
}
