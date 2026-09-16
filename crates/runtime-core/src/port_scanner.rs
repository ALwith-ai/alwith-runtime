use std::collections::{HashMap, HashSet};

use netstat2::{AddressFamilyFlags, ProtocolFlags, ProtocolSocketInfo, TcpState, get_sockets_info};
use sysinfo::{ProcessesToUpdate, System};

pub fn listening_ports_by_root(root_process_ids: &[u32]) -> Result<HashMap<u32, Vec<u16>>, String> {
    let process_ids_by_root = descendant_process_ids_by_root(root_process_ids);
    let sockets = get_sockets_info(AddressFamilyFlags::IPV4 | AddressFamilyFlags::IPV6, ProtocolFlags::TCP)
        .map_err(|error| error.to_string())?;
    let mut ports_by_root =
        root_process_ids.iter().map(|root_process_id| (*root_process_id, Vec::new())).collect::<HashMap<_, _>>();
    for socket in sockets {
        let port = match socket.protocol_socket_info {
            ProtocolSocketInfo::Tcp(tcp) if tcp.state == TcpState::Listen => tcp.local_port,
            ProtocolSocketInfo::Tcp(_) | ProtocolSocketInfo::Udp(_) => continue,
        };
        for (root_process_id, process_ids) in &process_ids_by_root {
            if socket.associated_pids.iter().any(|process_id| process_ids.contains(process_id)) {
                ports_by_root.get_mut(root_process_id).expect("root port list").push(port);
            }
        }
    }
    for ports in ports_by_root.values_mut() {
        ports.sort_unstable();
        ports.dedup();
    }
    Ok(ports_by_root)
}

fn descendant_process_ids_by_root(root_process_ids: &[u32]) -> HashMap<u32, HashSet<u32>> {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::All, false);
    let mut children_by_parent = HashMap::<u32, Vec<u32>>::new();
    for (process_id, process) in system.processes() {
        if let Some(parent) = process.parent() {
            children_by_parent.entry(parent.as_u32()).or_default().push(process_id.as_u32());
        }
    }
    root_process_ids
        .iter()
        .map(|root_process_id| {
            let mut process_ids = HashSet::from([*root_process_id]);
            let mut pending = vec![*root_process_id];
            while let Some(process_id) = pending.pop() {
                for child_process_id in children_by_parent.get(&process_id).into_iter().flatten() {
                    if process_ids.insert(*child_process_id) {
                        pending.push(*child_process_id);
                    }
                }
            }
            (*root_process_id, process_ids)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_process_is_in_its_own_tree() {
        let process_id = std::process::id();
        assert!(
            descendant_process_ids_by_root(&[process_id])
                .get(&process_id)
                .expect("current process tree")
                .contains(&process_id)
        );
    }

    #[test]
    fn finds_listening_port_owned_by_current_process() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind test listener");
        let port = listener.local_addr().expect("test listener address").port();
        let process_id = std::process::id();
        assert!(
            listening_ports_by_root(&[process_id])
                .expect("scan test listener")
                .get(&process_id)
                .expect("current process ports")
                .contains(&port)
        );
    }
}
