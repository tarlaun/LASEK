def file = new File( request.getOutputDirectory(), request.getArtifactId()+"/bin/beast" );
file.setExecutable(true, false);

