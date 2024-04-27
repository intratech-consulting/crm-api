pipeline {
  agent any
  stages {
    stage('Connect to the test VM') {
      steps {
        sh 'ssh group2@10.2.160.51'
      }
    }

    stage('Change directory') {
      steps {
        dir(path: '~/TestServer/crm/crm-api')
      }
    }

  }
}