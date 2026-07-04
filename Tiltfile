for svc in ['gateway', 'order', 'inventory', 'payment', 'notification']:
    docker_build(svc, '.', dockerfile='Dockerfile', build_args={'SERVICE': svc})

k8s_yaml(kustomize('deploy/k8s'))

k8s_resource('gateway', port_forwards='8080:8080')   # public edge
k8s_resource('jaeger', port_forwards='16686:16686')  # tracing UI
for svc in ['order', 'inventory', 'payment', 'notification']:
    k8s_resource(svc)
