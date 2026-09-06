//go:build darwin && cgo

package custody

/*
#cgo LDFLAGS: -lproc -lbsm
#include <errno.h>
#include <stdint.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <mach/mach.h>
#include <mach/task_info.h>
#include <libproc.h>
#include <sys/proc_info.h>
#include <bsm/libbsm.h>

struct ao_identity {
 uint32_t token[8], ppid, pgid, uid, status;
 uint64_t sec, usec;
 char path[PROC_PIDPATHINFO_MAXSIZE];
};
static int ao_token(int pid, audit_token_t *token) {
 mach_port_t task = MACH_PORT_NULL;
 kern_return_t kr = task_name_for_pid(mach_task_self(), pid, &task);
 if (kr != KERN_SUCCESS) return (int)kr;
 mach_msg_type_number_t count=TASK_AUDIT_TOKEN_COUNT;
 kr=task_info(task,TASK_AUDIT_TOKEN,(task_info_t)token,&count);
 mach_port_deallocate(mach_task_self(),task);
 return kr==KERN_SUCCESS && count==TASK_AUDIT_TOKEN_COUNT ? 0 : (int)(kr ? kr : KERN_FAILURE);
}
static int ao_observe(int pid, struct ao_identity *out) {
 audit_token_t before={0}, after={0};
 int result=ao_token(pid,&before); if(result) return result;
 struct proc_bsdinfo info={0};
 if(proc_pidinfo(pid,PROC_PIDTBSDINFO,0,&info,sizeof(info))!=sizeof(info)) return errno ? errno : EIO;
 if(proc_pidpath(pid,out->path,sizeof(out->path))<=0) return errno ? errno : EIO;
 result=ao_token(pid,&after); if(result) return result;
 for(int i=0;i<8;i++) { if(before.val[i]!=after.val[i]) return ESTALE; out->token[i]=before.val[i]; }
 if(audit_token_to_pid(before)!=pid || info.pbi_pid!=pid || audit_token_to_euid(before)!=info.pbi_uid) return ESTALE;
 out->ppid=info.pbi_ppid;out->pgid=info.pbi_pgid;out->uid=info.pbi_uid;out->status=info.pbi_status;
 out->sec=info.pbi_start_tvsec;out->usec=info.pbi_start_tvusec;
 return 0;
}
static int ao_signal(uint32_t *values, int resume) {
 audit_token_t token={0};for(int i=0;i<8;i++) token.val[i]=values[i];
 if(audit_token_to_pid(token)<=1 || audit_token_to_euid(token)!=geteuid()) return EPERM;
 errno=0;int rc=proc_signal_with_audittoken(&token,resume ? SIGCONT : SIGSTOP);
 return rc==0 ? 0 : (errno ? errno : rc);
}
static int ao_topology(int pid,struct ao_identity *out) {
 struct proc_bsdinfo info={0};
 if(proc_pidinfo(pid,PROC_PIDTBSDINFO,0,&info,sizeof(info))!=sizeof(info)) return errno ? errno : EIO;
 out->ppid=info.pbi_ppid;out->pgid=info.pbi_pgid;out->uid=info.pbi_uid;out->status=info.pbi_status;
 return 0;
}
static int ao_pids(int *pids,int size) {return proc_listallpids(pids,size);}
*/
import "C"

import (
	"fmt"
	"os/exec"
	"syscall"
	"unsafe"
)

func ProcessCapability() error { return nil }

func ObserveProcess(pid int) (ProcessIdentity, error) {
	if pid <= 1 {
		return ProcessIdentity{}, ErrUnknown
	}
	var info C.struct_ao_identity
	if rc := C.ao_observe(C.int(pid), &info); rc != 0 {
		return ProcessIdentity{}, fmt.Errorf("%w: observe pid %d: kernel result %d", ErrUnknown, pid, rc)
	}
	out := ProcessIdentity{PID: pid, ParentPID: int(info.ppid), GroupID: int(info.pgid), UID: uint32(info.uid), Status: uint32(info.status), Executable: C.GoString(&info.path[0]), StartSeconds: uint64(info.sec), StartMicroseconds: uint64(info.usec)}
	for i := range out.AuditToken {
		out.AuditToken[i] = uint32(info.token[i])
	}
	return out, nil
}

// SignalProcess never rebinds an old identity to a new PID generation.
func SignalProcess(identity ProcessIdentity, resume bool) error {
	if identity.PID <= 1 || int(identity.AuditToken[5]) != identity.PID {
		return ErrUnknown
	}
	var token [8]C.uint32_t
	for i, v := range identity.AuditToken {
		token[i] = C.uint32_t(v)
	}
	var cont C.int
	if resume {
		cont = 1
	}
	if rc := C.ao_signal(&token[0], cont); rc != 0 {
		return fmt.Errorf("%w: signal pid %d generation %d: kernel result %d", ErrUnknown, identity.PID, identity.AuditToken[7], rc)
	}
	return nil
}

// ProcessInventory reads OS process identities. Failed observations are retained
// as unknown PID entries so callers cannot infer absence from a failed probe.
func ProcessInventory() ([]ProcessIdentity, []int, error) {
	n := int(C.ao_pids(nil, 0))
	if n <= 0 {
		return nil, nil, ErrUnknown
	}
	pids := make([]C.int, n+256)
	got := int(C.ao_pids(&pids[0], C.int(len(pids)*int(unsafe.Sizeof(pids[0])))))
	if got <= 0 || got >= len(pids) {
		return nil, nil, ErrUnknown
	}
	out := []ProcessIdentity{}
	unknown := []int{}
	for _, pid := range pids[:got] {
		if pid <= 1 {
			continue
		}
		var info C.struct_ao_identity
		if C.ao_topology(pid, &info) != 0 {
			// A PID may exit between enumeration and its BSD lookup. It is
			// still reported unknown; identity-bound tracked members are
			// separately checked, never inferred to have exited from this.
			unknown = append(unknown, int(pid))
			continue
		}
		out = append(out, ProcessIdentity{PID: int(pid), ParentPID: int(info.ppid), GroupID: int(info.pgid), UID: uint32(info.uid), Status: uint32(info.status)})
	}
	return out, unknown, nil
}

// ProcessGone requires a positive OS outcome: ESRCH, or a different execution
// generation at that PID. Permission/inspection failures remain unknown.
func ProcessGone(p ProcessIdentity) (bool, error) {
	var info C.struct_ao_identity
	rc := C.ao_topology(C.int(p.PID), &info)
	if rc == C.ESRCH {
		return true, nil
	}
	if rc != 0 {
		return false, ErrUnknown
	}
	actual, err := ObserveProcess(p.PID)
	if err != nil {
		return false, err
	}
	return !actual.Same(p), nil
}

func preparationCommand(binary string, args ...string) (*exec.Cmd, error) {
	argv := append([]string{"-c", `IFS= read -r ao_start || exit; "$@"; ao_status=$?; exit "$ao_status"`, "ao-managed-preparation", binary}, args...)
	cmd := exec.Command("/bin/sh", argv...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd, nil
}

func preservationSpace(path string, required int64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return err
	}
	if uint64(required) > uint64(stat.Bavail)*uint64(stat.Bsize) {
		return fmt.Errorf("%w: preservation requires %d free bytes", ErrUnknown, required)
	}
	return nil
}
