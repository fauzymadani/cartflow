for svc in ['gateway', 'order', 'inventory', 'payment', 'notification']:
    docker_build(svc, '.', dockerfile='Dockerfile', build_args={'SERVICE': svc})

k8s_yaml(kustomize('deploy/k8s'))

k8s_resource('gateway', port_forwards='8080:8080')  # the only public edge
for svc in ['order', 'inventory', 'payment', 'notification']:
    k8s_resource(svc)
