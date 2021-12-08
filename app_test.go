package main

import (
	"errors"
	"fmt"
	"testing"
)

func TestA(t *testing.T) {
	err := grandParentMethod("a1")
	var e flagErr
	// As 第二个参数是用来存放提取出来的 err 的指针
	// 会把最内层的 error 提取出来
	if errors.As(err, &e) {
		fmt.Printf("err: %v, e: %v\n", err, e)
		//print
		//err: error of grand parent method, orginal: error of parent method, orginal err: error of flag, e: error of flag

		err := errors.Unwrap(err)
		fmt.Printf("unwrap1: %v\n", err)
		//unwrap1: error of parent method, orginal err: error of flag
		err = errors.Unwrap(err)
		fmt.Printf("unwrap2: %v\n", err)
		//unwrap2: error of flag
		err = errors.Unwrap(err)
		fmt.Printf("unwrap3: %v\n", err)
		// unwrap3: <nil>
	}
}


type flagErr struct {
	Msg string
}

func (e flagErr) Error() string {
	return e.Msg
}

func grandParentMethod(flag string) error {
	err := parentMethod(flag)
	if err != nil {
		err := fmt.Errorf("error of grand parent method, orginal: %w", err)
		fmt.Printf("grandParentMethod: type: %T, value: %v\n", err, err)
		//print
		//grandParentMethod: type: *fmt.wrapError, value: error off grand parent method, orginal: error of parent method, orginal err: error of flag
		return err
	}
	return nil
}

func parentMethod(flag string) error {
	_, err := doSth(flag)
	if err != nil {
		//包装错误时, 使用 %w
		err := fmt.Errorf("error of parent method, orginal err: %w", err)
		fmt.Printf("parentMethod: type: %T, value: %v\n", err, err)
		//print
		//parentMethod: type: *fmt.wrapError, value: error of parent method, orginal err: error of flag
		return err
	}
	return nil
}

func doSth(flag string) (string, error) {
	if flag != "a" {
		err := flagErr{"error of flag"}
		fmt.Printf("doSth: type: %T, value: %v\n", err, err)
		//print
		//doSth: type: main.flagErr, value: error of flag
		return "hello", err
	}
	return flag, nil
}

